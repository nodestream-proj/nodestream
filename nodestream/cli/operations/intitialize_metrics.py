from typing import Any

from ...metrics import (
    AggregateHandler,
    ConsoleMetricHandler,
    JsonLogMetricHandler,
    MetricHandler,
    Metrics,
    PrometheusMetricHandler,
)
from ..commands.nodestream_command import NodestreamCommand
from ..commands.shared_options import (
    PROMETHEUS_ADDRESS_OPTION,
    PROMETHEUS_CERTFILE_OPTION,
    PROMETHEUS_KEYFILE_OPTION,
    PROMETHEUS_OPTION,
    PROMETHEUS_PORT_OPTION,
)
from .operation import Operation


class InitializeMetricsHandler(Operation):
    async def perform(self, command: NodestreamCommand) -> Any:
        Metrics.get().handler = handler = self.get_metric_handler(command)
        handler.start()

    def get_metric_handler(self, command: NodestreamCommand) -> MetricHandler:
        handlers = []
        if command.has_json_logging_set:
            handlers.append(JsonLogMetricHandler())

        prometheus_option = command.option(PROMETHEUS_OPTION.name)
        if prometheus_option:
            handlers.append(
                PrometheusMetricHandler(
                    port=int(command.option(PROMETHEUS_PORT_OPTION.name)),
                    listen_address=command.option(PROMETHEUS_ADDRESS_OPTION.name),
                    certfile=command.option(PROMETHEUS_CERTFILE_OPTION.name),
                    keyfile=command.option(PROMETHEUS_KEYFILE_OPTION.name),
                )
            )

        return AggregateHandler(handlers) if handlers else ConsoleMetricHandler(command)
