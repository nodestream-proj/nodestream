import sys

import pytest

from nodestream.metrics import (
    RECORDS,
    AggregateHandler,
    ConsoleMetricHandler,
    JsonLogMetricHandler,
    Metric,
    Metrics,
    NullMetricHandler,
    PrometheusMetricHandler,
)


def test_metric_increment_on_handler_increments_metric_on_handler(mocker):
    handler = mocker.Mock()
    metric = RECORDS
    metric.increment_on(handler, 1)
    handler.increment.assert_called_once_with(metric, 1)


def test_metric_decrement_on_handler_decrements_metric_on_handler(mocker):
    handler = mocker.Mock()
    metric = RECORDS
    metric.decrement_on(handler, 1)
    handler.decrement.assert_called_once_with(metric, 1)


def test_prometheus_metric_handler_registers_new_metrics(mocker):
    handler = PrometheusMetricHandler()
    metric = Metric("new_metric", "New metric")
    metric.register(handler)
    assert metric in handler.instruments_by_metric.keys()


def test_null_metric_handler():
    handler = NullMetricHandler()
    handler.increment(RECORDS, 1)
    handler.decrement(RECORDS, 1)
    # No assertions needed as NullMetricHandler does nothing


def test_prometheus_metric_handler(mocker):
    mock_start_http_server = mocker.patch("nodestream.metrics.start_http_server")
    mock_start_http_server.return_value = (mocker.Mock(), mocker.Mock())
    handler = PrometheusMetricHandler()
    handler.start()
    mock_start_http_server.assert_called_once()
    handler.increment(RECORDS, 1)
    handler.decrement(RECORDS, 1)
    handler.stop()


def test_console_metric_handler(mocker):
    mock_command = mocker.Mock()
    handler = ConsoleMetricHandler(mock_command)
    handler.increment(RECORDS, 1)
    handler.decrement(RECORDS, 1)
    handler.tick()
    handler.stop()
    mock_command.table.assert_called_once()


def test_json_log_metric_handler(mocker):
    mock_logger = mocker.patch("nodestream.metrics.getLogger").return_value
    handler = JsonLogMetricHandler()
    handler.increment(RECORDS, 1)
    handler.decrement(RECORDS, 1)
    handler.tick()
    handler.stop()
    mock_logger.info.assert_called_once()


def test_aggregate_handler(mocker):
    mock_handler1 = mocker.Mock()
    mock_handler2 = mocker.Mock()
    handler = AggregateHandler([mock_handler1, mock_handler2])
    handler.start()
    mock_handler1.start.assert_called_once()
    mock_handler2.start.assert_called_once()
    handler.increment(RECORDS, 1)
    mock_handler1.increment.assert_called_once_with(RECORDS, 1)
    mock_handler2.increment.assert_called_once_with(RECORDS, 1)
    handler.decrement(RECORDS, 1)
    mock_handler1.decrement.assert_called_once_with(RECORDS, 1)
    mock_handler2.decrement.assert_called_once_with(RECORDS, 1)
    handler.stop()
    mock_handler1.stop.assert_called_once()
    mock_handler2.stop.assert_called_once()


def test_metrics_context(mocker):
    handler = mocker.Mock()
    with Metrics.capture(handler) as metrics:
        metrics.increment(RECORDS, 1)
        handler.increment.assert_called_once_with(RECORDS, 1)
    handler.stop.assert_called_once()


def test_metrics_get(mocker):
    handler = mocker.Mock()
    with Metrics.capture(handler):
        metrics = Metrics.get()
        assert metrics.handler == handler
    assert isinstance(Metrics.get().handler, NullMetricHandler)


def test_metrics_contextualize():
    metrics = Metrics()
    metrics.contextualize("new_scope", "new_pipeline")
    assert metrics.scope_name == "new_scope"
    assert metrics.pipeline_name == "new_pipeline"


def test_prometheus_metric_handler_import_error(mocker):
    # These are the hoops we must run through to test an ImportError
    # for the case where the prometheus_client library is not installed
    # and the PrometheusMetricHandler is instantiated. So patch the
    # prometheus_client module to None and delete the metrics module
    # from sys.modules to reload it.
    mocker.patch.dict("sys.modules", {"prometheus_client": None})
    del sys.modules["nodestream.metrics"]
    from nodestream.metrics import PrometheusMetricHandler

    with pytest.raises(ImportError) as excinfo:
        PrometheusMetricHandler()

    assert (
        "The prometheus_client library is required to use the PrometheusMetricHandler."
        in str(excinfo.value)
    )


def test_aggregate_handler_tick_calls_all_handlers(mocker):
    mock_handler1 = mocker.Mock()
    mock_handler2 = mocker.Mock()
    handler = AggregateHandler([mock_handler1, mock_handler2])
    handler.tick()
    mock_handler1.tick.assert_called_once()
    mock_handler2.tick.assert_called_once()


def test_metric_equality_and_hash():
    """Test Metric equality and hash methods"""
    metric1 = Metric("test_metric", "Test description")
    metric2 = Metric("test_metric", "Different description")
    metric3 = Metric("different_metric", "Test description")

    # Test __eq__ and __ne__
    assert metric1 == metric2  # Same name
    assert metric1 != metric3  # Different name

    # Test __str__
    assert str(metric1) == "test_metric"
    assert str(metric2) == "test_metric"
    assert str(metric3) == "different_metric"

    # Test __hash__
    assert hash(metric1) == hash(metric2)  # Same name should have same hash


def test_console_metric_handler_discharge_with_accumulate(mocker):
    """Test that ConsoleMetricHandler discharge resets accumulating metrics"""
    mock_command = mocker.Mock()
    handler = ConsoleMetricHandler(mock_command)

    accumulating_metric = Metric("test_accumulate", accumulate=True)
    non_accumulating_metric = Metric("test_no_accumulate", accumulate=False)

    handler.increment(accumulating_metric, 5)
    handler.increment(non_accumulating_metric, 3)

    result = handler.discharge()

    # Should return metric names as keys
    assert result["test_accumulate"] == 5
    assert result["test_no_accumulate"] == 3

    # Accumulating metric should be reset to 0, non-accumulating should remain
    assert handler.metrics[accumulating_metric] == 0
    assert handler.metrics[non_accumulating_metric] == 3


def test_json_log_metric_handler_discharge_with_accumulate(mocker):
    """Test that JsonLogMetricHandler discharge resets accumulating metrics"""
    handler = JsonLogMetricHandler()

    accumulating_metric = Metric("test_accumulate", accumulate=True)
    non_accumulating_metric = Metric("test_no_accumulate", accumulate=False)

    handler.increment(accumulating_metric, 10)
    handler.increment(non_accumulating_metric, 7)

    result = handler.discharge()

    # Should return metric names as keys
    assert result["test_accumulate"] == 10
    assert result["test_no_accumulate"] == 7

    # Accumulating metric should be reset to 0, non-accumulating should remain
    assert handler.metrics[accumulating_metric] == 0
    assert handler.metrics[non_accumulating_metric] == 7
