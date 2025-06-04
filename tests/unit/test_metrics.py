import sys

import pytest

from nodestream.metrics import (
    AggregateHandler,
    ConsoleMetricHandler,
    JsonLogMetricHandler,
    Metric,
    NodestreamMetricRegistry,
    Metrics,
    NullMetricHandler,
    PrometheusMetricHandler,
)


def test_metric_registry_contains_subclasses_in_all_metrics():
    for metric in NodestreamMetricRegistry.get_all_metrics().values():
        assert metric in NodestreamMetricRegistry._subclasses


def test_metric_increment_on_handler_increments_metric_on_handler(mocker):
    handler = mocker.Mock()
    metric = NodestreamMetricRegistry.RECORDS
    metric.increment_on(handler, 1)
    handler.increment.assert_called_once_with(metric, 1)


def test_metric_decrement_on_handler_decrements_metric_on_handler(mocker):
    handler = mocker.Mock()
    metric = NodestreamMetricRegistry.RECORDS
    metric.decrement_on(handler, 1)
    handler.decrement.assert_called_once_with(metric, 1)


def test_prometheus_metric_handler_registers_new_metrics(mocker):
    handler = PrometheusMetricHandler()
    metric = Metric("new_metric", "New metric")
    metric.register(handler)
    assert metric in handler.instruments_by_metric.keys()


def test_null_metric_handler():
    handler = NullMetricHandler()
    handler.increment(NodestreamMetricRegistry.RECORDS, 1)
    handler.decrement(NodestreamMetricRegistry.RECORDS, 1)
    # No assertions needed as NullMetricHandler does nothing


def test_prometheus_metric_handler(mocker):
    mock_start_http_server = mocker.patch("nodestream.metrics.start_http_server")
    mock_start_http_server.return_value = (mocker.Mock(), mocker.Mock())
    handler = PrometheusMetricHandler()
    handler.start()
    mock_start_http_server.assert_called_once()
    handler.increment(NodestreamMetricRegistry.RECORDS, 1)
    handler.decrement(NodestreamMetricRegistry.RECORDS, 1)
    handler.stop()


def test_console_metric_handler(mocker):
    mock_command = mocker.Mock()
    handler = ConsoleMetricHandler(mock_command)
    handler.increment(NodestreamMetricRegistry.RECORDS, 1)
    handler.decrement(NodestreamMetricRegistry.RECORDS, 1)
    handler.tick()
    handler.stop()
    mock_command.table.assert_called_once()


def test_json_log_metric_handler(mocker):
    mock_logger = mocker.patch("nodestream.metrics.getLogger").return_value
    handler = JsonLogMetricHandler()
    handler.increment(NodestreamMetricRegistry.RECORDS, 1)
    handler.decrement(NodestreamMetricRegistry.RECORDS, 1)
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
    handler.increment(NodestreamMetricRegistry.RECORDS, 1)
    mock_handler1.increment.assert_called_once_with(NodestreamMetricRegistry.RECORDS, 1)
    mock_handler2.increment.assert_called_once_with(NodestreamMetricRegistry.RECORDS, 1)
    handler.decrement(NodestreamMetricRegistry.RECORDS, 1)
    mock_handler1.decrement.assert_called_once_with(NodestreamMetricRegistry.RECORDS, 1)
    mock_handler2.decrement.assert_called_once_with(NodestreamMetricRegistry.RECORDS, 1)
    handler.stop()
    mock_handler1.stop.assert_called_once()
    mock_handler2.stop.assert_called_once()


def test_metrics_context(mocker):
    handler = mocker.Mock()
    with Metrics.capture(handler) as metrics:
        metrics.increment(NodestreamMetricRegistry.RECORDS, 1)
        handler.increment.assert_called_once_with(NodestreamMetricRegistry.RECORDS, 1)
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
