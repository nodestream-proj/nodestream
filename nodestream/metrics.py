from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from logging import getLogger
from typing import Dict, Optional, Union

from cleo.commands.command import Command

Number = Union[int, float]


@dataclass(frozen=True)
class Metric:
    name: str
    description: Optional[str] = None
    accumulate: bool = False

    def increment_on(self, handler: "MetricHandler", value: Number = 1):
        """Increment this metric on the given handler."""
        handler.increment(self, value)

    def decrement_on(self, handler: "MetricHandler", value: Number = 1):
        """Decrement this metric on the given handler."""
        handler.decrement(self, value)

    def register(self, handler: "MetricHandler"):
        handler.increment(self, 0)
        return self

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return self.name != other.name


class MetricHandler(ABC):
    """Recieves metrics and handles them in some way.

    A metric handler is a class that recieves metrics and handles them in some way.
    """

    def start(self):
        """Start the handler.

        This method is called before any metrics are sent to the handler.
        It can be used to initialize resources or start background threads.
        """
        pass

    def stop(self):
        """Stop the handler.

        This method is called after all metrics have been sent to the handler.
        It can be used to release resources or stop background threads.
        """
        pass

    @abstractmethod
    def increment(self, metric: Metric, value: Number): ...

    @abstractmethod
    def decrement(self, metric: Metric, value: Number): ...

    @abstractmethod
    def tick(self): ...


class NullMetricHandler(MetricHandler):
    """A metric handler that does nothing."""

    def increment(self, _: Metric, __: Number):
        pass

    def decrement(self, _: Metric, __: Number):
        pass

    def tick(self):
        pass


# Core metrics
RECORDS = Metric("records", "Number of records processed", accumulate=True)
NON_FATAL_ERRORS = Metric(
    "non_fatal_errors", "Number of non-fatal errors", accumulate=True
)
FATAL_ERRORS = Metric("fatal_errors", "Number of fatal errors", accumulate=True)
NODES_UPSERTED = Metric(
    "nodes_upserted", "Number of nodes upserted to the graph", accumulate=True
)
RELATIONSHIPS_UPSERTED = Metric(
    "relationships_upserted",
    "Number of relationships upserted to the graph",
    accumulate=True,
)
TIME_TO_LIVE_OPERATIONS = Metric(
    "time_to_live_operations",
    "Number of time-to-live operations executed",
    accumulate=True,
)
INGEST_HOOKS_EXECUTED = Metric(
    "ingest_hooks_executed",
    "Number of ingest hooks executed to the graph",
    accumulate=True,
)
STEPS_RUNNING = Metric(
    "steps_running", "Number of steps currently running in the pipeline"
)


try:

    from prometheus_client import REGISTRY, Gauge, start_http_server

    class PrometheusMetricHandler(MetricHandler):
        """A metric handler that sends metrics to Prometheus."""

        def __init__(
            self,
            port: int = 9090,
            listen_address: str = "0.0.0.0",
            certfile: Optional[str] = None,
            keyfile: Optional[str] = None,
        ):
            self.logger = getLogger(__name__)
            self.instruments_by_metric: Dict[Metric, Gauge] = {}
            self.port = port
            self.listen_address = listen_address
            self.certfile = certfile
            self.keyfile = keyfile

        def start(self):
            server, thread = start_http_server(
                port=self.port,
                addr=self.listen_address,
                certfile=self.certfile,
                keyfile=self.keyfile,
            )
            self.server = server
            self.thread = thread
            self.logger.info(
                f"Prometheus metrics server started on {self.listen_address}:{self.port}"
            )

        def stop(self):
            for instrument in self.instruments_by_metric.values():
                REGISTRY.unregister(instrument)
            self.logger.info("Shutting down prometheus metrics server")
            self.server.shutdown()
            self.logger.debug("Waiting for prometheus metrics server to shut down")
            self.thread.join()
            self.logger.info("Prometheus metrics server shut down successfully")

        def get_gauge(self, metric: Metric) -> Gauge:
            """Get the Gauge for the given metric, creating it if it doesn't exist."""
            if metric not in self.instruments_by_metric:
                self.instruments_by_metric[metric] = Gauge(
                    metric.name,
                    metric.description or "",
                    registry=REGISTRY,
                )
            return self.instruments_by_metric[metric]

        def increment(self, metric: Metric, value: Number):
            self.get_gauge(metric).inc(value)

        def decrement(self, metric: Metric, value: Number):
            self.get_gauge(metric).dec(value)

        def tick(self):
            pass

except ImportError:

    class PrometheusMetricHandler(MetricHandler):
        """A metric handler that sends metrics to Prometheus."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "The prometheus_client library is required to use the PrometheusMetricHandler. "
                "Please install the `prometheus` extra."
            )

        def increment(self, metric: Metric, value: Number):
            pass

        def decrement(self, metric: Metric, value: Number):
            pass

        def tick(self):
            pass


STATS_TABLE_COLS = ["Statistic", "Value"]


class ConsoleMetricHandler(MetricHandler):
    """A metric handler that prints metrics to the console."""

    # Note: TODO: Tie incremental values to the metrics rather than the handler.

    def __init__(self, command: Command):
        self.metrics: dict[Metric, Number] = {}
        self.command = command

    def increment(self, metric: Metric, value: Number):
        self.metrics[metric] = self.metrics.get(metric, 0) + value

    def decrement(self, metric: Metric, value: Number):
        self.metrics[metric] = self.metrics.get(metric, 0) - value

    def discharge(self) -> dict[Metric, Number]:
        metrics = {}
        for metric, value in self.metrics.items():
            metrics[metric.name] = value
            if metric.accumulate:
                self.metrics[metric] = 0
        return metrics

    def render(self):
        metrics = self.discharge()
        stats = ((k, str(v)) for k, v in metrics.items())
        table = self.command.table(STATS_TABLE_COLS, stats)
        table.render()

    def tick(self):
        self.render()

    def stop(self):
        pass


class JsonLogMetricHandler(MetricHandler):
    """A metric handler that logs metrics in JSON format."""

    def __init__(self):
        self.metrics: dict[Metric, Number] = {}
        self.logger = getLogger(__name__)

    def increment(self, metric: Metric, value: Number):
        self.metrics[metric] = self.metrics.get(metric, 0) + value

    def decrement(self, metric: Metric, value: Number):
        self.metrics[metric] = self.metrics.get(metric, 0) - value

    def discharge(self) -> dict[Metric, Number]:
        metrics = {}
        for metric, value in self.metrics.items():
            metrics[metric.name] = value
            if metric.accumulate:
                self.metrics[metric] = 0

        return metrics

    def render(self):
        metrics = self.discharge()
        self.logger.info(
            "Metrics Report",
            extra=metrics,
        )

    def stop(self):
        pass

    def tick(self):
        self.render()


class AggregateHandler(MetricHandler):
    """A metric handler that aggregates metrics from multiple handlers."""

    def __init__(self, handlers):
        self.handlers = handlers

    def start(self):
        for handler in self.handlers:
            handler.start()

    def stop(self):
        for handler in self.handlers:
            handler.stop()

    def increment(self, metric: Metric, value: Number):
        for handler in self.handlers:
            handler.increment(metric, value)

    def decrement(self, metric: Metric, value: Number):
        for handler in self.handlers:
            handler.decrement(metric, value)

    def tick(self):
        for handler in self.handlers:
            handler.tick()


UNKNOWN_PIPELINE_NAME = "unknown"
UNKNOWN_PIPELINE_SCOPE = "unknown"
context: ContextVar["Metrics"] = ContextVar("context")


class Metrics:
    def __init__(
        self,
        handler: Optional[MetricHandler] = None,
        scope_name: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ):
        self.handler = handler or NullMetricHandler()
        self.scope_name = scope_name or UNKNOWN_PIPELINE_SCOPE
        self.pipeline_name = pipeline_name or UNKNOWN_PIPELINE_NAME

    def increment(self, metric: Metric, value: Number = 1):
        metric.increment_on(self.handler, value)

    def decrement(self, metric: Metric, value: Number = 1):
        metric.decrement_on(self.handler, value)

    def tick(self):
        self.handler.tick()

    def start(self):
        self.handler.start()

    def stop(self):
        self.handler.stop()

    @staticmethod
    @contextmanager
    def capture(handler: Optional[MetricHandler] = None):
        metrics = Metrics(handler)
        metrics.start()
        token = context.set(metrics)
        try:
            yield metrics
        finally:
            context.reset(token)
            metrics.stop()

    @staticmethod
    def get() -> "Metrics":
        try:
            return context.get()
        except LookupError:
            return Metrics()

    def contextualize(self, scope_name: str, pipeline_name: str):
        self.scope_name = scope_name
        self.pipeline_name = pipeline_name
