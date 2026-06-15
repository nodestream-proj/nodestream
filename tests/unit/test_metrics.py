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


def test_metric_increment_on_handler():
    handler = JsonLogMetricHandler()
    RECORDS.increment_on(handler, 5)
    assert handler.metrics[RECORDS] == 5
    RECORDS.increment_on(handler, 3)
    assert handler.metrics[RECORDS] == 8


def test_metric_decrement_on_handler():
    handler = JsonLogMetricHandler()
    gauge = Metric("gauge", accumulate=False)
    gauge.increment_on(handler, 10)
    gauge.decrement_on(handler, 3)
    assert handler.metrics[gauge] == 7


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
    handler = ConsoleMetricHandler(mocker.Mock())
    gauge = Metric("gauge", accumulate=False)
    handler.increment(gauge, 5)
    assert handler.metrics[gauge] == 5
    handler.decrement(gauge, 2)
    assert handler.metrics[gauge] == 3
    # tick() calls render() which discharges metrics — non-accumulating
    # metrics keep their value after discharge.
    handler.tick()
    assert handler.metrics[gauge] == 3
    handler.stop()


def test_json_log_metric_handler():
    handler = JsonLogMetricHandler()
    gauge = Metric("gauge", accumulate=False)
    handler.increment(gauge, 5)
    assert handler.metrics[gauge] == 5
    handler.decrement(gauge, 2)
    assert handler.metrics[gauge] == 3
    handler.stop()


def test_aggregate_handler():
    handler1 = JsonLogMetricHandler()
    handler2 = JsonLogMetricHandler()
    aggregate = AggregateHandler([handler1, handler2])
    gauge = Metric("gauge", accumulate=False)
    aggregate.increment(gauge, 5)
    # Both sub-handlers should reflect the increment.
    assert handler1.metrics[gauge] == 5
    assert handler2.metrics[gauge] == 5
    aggregate.decrement(gauge, 2)
    assert handler1.metrics[gauge] == 3
    assert handler2.metrics[gauge] == 3
    aggregate.set_value(gauge, 99)
    assert handler1.metrics[gauge] == 99
    assert handler2.metrics[gauge] == 99


def test_metrics_context():
    handler = JsonLogMetricHandler()
    gauge = Metric("gauge", accumulate=False)
    with Metrics.capture(handler) as metrics:
        metrics.increment(gauge, 3)
        assert handler.metrics[gauge] == 3
        metrics.decrement(gauge, 1)
        assert handler.metrics[gauge] == 2


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


def test_aggregate_handler_tick_renders_sub_handlers():
    handler1 = JsonLogMetricHandler()
    handler2 = JsonLogMetricHandler()
    aggregate = AggregateHandler([handler1, handler2])
    # Increment then tick — discharge should reset accumulating metrics.
    metric = Metric("test_acc", accumulate=True)
    aggregate.increment(metric, 10)
    aggregate.tick()
    # After tick, accumulating metrics should be discharged (reset to 0).
    assert handler1.metrics[metric] == 0
    assert handler2.metrics[metric] == 0


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


def test_console_metric_handler_discharge_with_accumulate():
    """Test that ConsoleMetricHandler discharge resets accumulating metrics"""
    handler = ConsoleMetricHandler(command=None)

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


def test_json_log_metric_handler_discharge_with_accumulate():
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


def test_json_log_metric_handler_discharge_with_accumulate_and_decrement():
    """Test that JsonLogMetricHandler does not decrement accumulating metrics"""
    handler = JsonLogMetricHandler()

    accumulating_metric = Metric("test_accumulate", accumulate=True)
    non_accumulating_metric = Metric("test_no_accumulate", accumulate=False)

    handler.increment(accumulating_metric, 10)
    handler.decrement(accumulating_metric, 7)

    handler.increment(non_accumulating_metric, 10)
    handler.decrement(non_accumulating_metric, 7)

    # Accumulating metric should be reset to 0, non-accumulating should remain
    assert handler.metrics[accumulating_metric] == 10
    assert handler.metrics[non_accumulating_metric] == 3


def test_console_metric_handler_discharge_with_accumulate_and_decrement():
    """Test that ConsoleMetricHandler does not decrement accumulating metrics"""
    handler = ConsoleMetricHandler(command=None)

    accumulating_metric = Metric("test_accumulate", accumulate=True)
    non_accumulating_metric = Metric("test_no_accumulate", accumulate=False)

    handler.increment(accumulating_metric, 10)
    handler.decrement(accumulating_metric, 7)

    handler.increment(non_accumulating_metric, 10)
    handler.decrement(non_accumulating_metric, 7)

    # Accumulating metric should be reset to 0, non-accumulating should remain
    assert handler.metrics[accumulating_metric] == 10
    assert handler.metrics[non_accumulating_metric] == 3


def test_null_metric_handler_set_value():
    handler = NullMetricHandler()
    # Should not raise — it's a no-op.
    handler.set_value(RECORDS, 42)


def test_console_metric_handler_set_value():
    handler = ConsoleMetricHandler(command=None)
    metric = Metric("gauge_metric", accumulate=False)
    handler.set_value(metric, 99)
    assert handler.metrics[metric] == 99
    # Overwrite with a new value.
    handler.set_value(metric, 0)
    assert handler.metrics[metric] == 0


def test_json_log_metric_handler_set_value():
    handler = JsonLogMetricHandler()
    metric = Metric("gauge_metric", accumulate=False)
    handler.set_value(metric, 42)
    assert handler.metrics[metric] == 42


def test_aggregate_handler_set_value():
    handler1 = JsonLogMetricHandler()
    handler2 = JsonLogMetricHandler()
    aggregate = AggregateHandler([handler1, handler2])
    metric = Metric("gauge_metric", accumulate=False)
    aggregate.set_value(metric, 7)
    assert handler1.metrics[metric] == 7
    assert handler2.metrics[metric] == 7


def test_metrics_set_value():
    """Metrics.set_value should update the handler's internal state."""
    handler = ConsoleMetricHandler(command=None)
    with Metrics.capture(handler) as metrics:
        metrics.set_value(RECORDS, 42)
        assert handler.metrics[RECORDS] == 42
        metrics.set_value(RECORDS, 0)
        assert handler.metrics[RECORDS] == 0


def test_metric_set_value_on_handler():
    """Metric.set_value should update the handler's metric state."""
    handler = JsonLogMetricHandler()
    metric = Metric("gauge", accumulate=False)
    metric.set_value(10, handler)
    assert handler.metrics[metric] == 10


def test_console_metric_handler_render_no_op_when_no_command():
    """ConsoleMetricHandler.render() should be a no-op when command is None."""
    handler = ConsoleMetricHandler(command=None)
    handler.increment(RECORDS, 5)
    # Should not raise even though command is None
    handler.tick()
    # Value is unchanged — discharge did not run because render returned early
    assert handler.metrics[RECORDS] == 5
