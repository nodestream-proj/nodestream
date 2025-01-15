from nodestream.metrics import (
    AggregateHandler,
    ConsoleMetricHandler,
    JsonLogMetricHandler,
    Metric,
    Metrics,
    NullMetricHandler,
    PrometheusMetricHandler,
)


def test_null_metric_handler():
    handler = NullMetricHandler()
    handler.increment(Metric.RECORDS, 1)
    handler.decrement(Metric.RECORDS, 1)
    # No assertions needed as NullMetricHandler does nothing


def test_prometheus_metric_handler(mocker):
    mock_start_http_server = mocker.patch("nodestream.metrics.start_http_server")
    mock_start_http_server.return_value = (mocker.Mock(), mocker.Mock())
    handler = PrometheusMetricHandler()
    handler.start()
    mock_start_http_server.assert_called_once()
    handler.increment(Metric.RECORDS, 1)
    handler.decrement(Metric.RECORDS, 1)
    handler.stop()


def test_console_metric_handler(mocker):
    mock_command = mocker.Mock()
    handler = ConsoleMetricHandler(mock_command)
    handler.increment(Metric.RECORDS, 1)
    handler.decrement(Metric.RECORDS, 1)
    handler.stop()
    mock_command.table.assert_called_once()


def test_json_log_metric_handler(mocker):
    mock_logger = mocker.patch("nodestream.metrics.getLogger").return_value
    handler = JsonLogMetricHandler()
    handler.increment(Metric.RECORDS, 1)
    handler.decrement(Metric.RECORDS, 1)
    handler.stop()
    mock_logger.info.assert_called_once()


def test_aggregate_handler(mocker):
    mock_handler1 = mocker.Mock()
    mock_handler2 = mocker.Mock()
    handler = AggregateHandler([mock_handler1, mock_handler2])
    handler.start()
    mock_handler1.start.assert_called_once()
    mock_handler2.start.assert_called_once()
    handler.increment(Metric.RECORDS, 1)
    mock_handler1.increment.assert_called_once_with(Metric.RECORDS, 1)
    mock_handler2.increment.assert_called_once_with(Metric.RECORDS, 1)
    handler.decrement(Metric.RECORDS, 1)
    mock_handler1.decrement.assert_called_once_with(Metric.RECORDS, 1)
    mock_handler2.decrement.assert_called_once_with(Metric.RECORDS, 1)
    handler.stop()
    mock_handler1.stop.assert_called_once()
    mock_handler2.stop.assert_called_once()


def test_metrics_context(mocker):
    handler = mocker.Mock()
    with Metrics.capture(handler) as metrics:
        metrics.increment(Metric.RECORDS, 1)
        handler.increment.assert_called_once_with(Metric.RECORDS, 1)
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
