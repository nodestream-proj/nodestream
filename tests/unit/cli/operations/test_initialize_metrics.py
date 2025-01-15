import pytest
from nodestream.cli.operations.intitialize_metrics import InitializeMetricsHandler
from nodestream.cli.commands.nodestream_command import NodestreamCommand

from nodestream.metrics import (
    Metrics,
    PrometheusMetricHandler,
    ConsoleMetricHandler,
    JsonLogMetricHandler,
    AggregateHandler,
)

SET_PROMETHEUS_OPTIONS = {
    "prometheus": True,
    "prometheus-listen-port": "9090",
    "prometheus-listen-address": "0.0.0.0",
    "prometheus-certfile": None,
    "prometheus-keyfile": None,
}


@pytest.fixture
def command(mocker):
    with Metrics.capture():
        command = mocker.Mock(spec=NodestreamCommand)
        command.has_json_logging_set = False
        command.option = mocker.Mock(side_effect=lambda name: None)
        yield command


@pytest.fixture
def operation():
    return InitializeMetricsHandler()


@pytest.mark.asyncio
async def test_perform(operation, command, mocker):
    command.has_json_logging_set = True
    command.option.side_effect = SET_PROMETHEUS_OPTIONS.get
    operation.get_metric_handler = mocker.Mock()

    await operation.perform(command)

    operation.get_metric_handler.assert_called_once_with(command)
    operation.get_metric_handler.return_value.start.assert_called_once()


def test_get_metric_handler_with_json_logging(operation, command):
    command.has_json_logging_set = True
    metric_handler = operation.get_metric_handler(command)
    assert isinstance(metric_handler, AggregateHandler)
    assert any(isinstance(h, JsonLogMetricHandler) for h in metric_handler.handlers)
    metric_handler.start()
    metric_handler.stop()


def test_get_metric_handler_with_prometheus(operation, command):
    command.option.side_effect = SET_PROMETHEUS_OPTIONS.get
    metric_handler = operation.get_metric_handler(command)
    assert isinstance(metric_handler, AggregateHandler)
    assert any(isinstance(h, PrometheusMetricHandler) for h in metric_handler.handlers)
    metric_handler.start()
    metric_handler.stop()


def test_get_metric_handler_with_no_options(operation, command):
    metric_handler = operation.get_metric_handler(command)
    assert isinstance(metric_handler, ConsoleMetricHandler)
