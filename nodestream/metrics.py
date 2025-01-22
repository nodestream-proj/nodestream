from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum, auto
from logging import getLogger
from typing import Optional, Union

from cleo.commands.command import Command

Number = Union[int, float]


class Metric(Enum):
    RECORDS = auto()
    NODES_UPSERTED = auto()
    RELATIONSHIPS_UPSERTED = auto()
    TIME_TO_LIVE_OPERATIONS = auto()
    INGEST_HOOKS_EXECUTED = auto()
    NON_FATAL_ERRORS = auto()
    STEPS_RUNNING = auto()
    BUFFERED_RECORDS = auto()


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


class NullMetricHandler(MetricHandler):
    """A metric handler that does nothing."""

    def increment(self, _: Metric, __: Number):
        pass

    def decrement(self, _: Metric, __: Number):
        pass


RECORDS_COUNTER_NAME = "records"
NON_FATAL_ERRORS_COUNTER_NAME = "non_fatal_errors"
NODES_UPSERTED_COUNTER_NAME = "nodes_upserted"
RELS_UPSERTED_COUNTER_NAME = "relationships_upserted"
TTL_OPERATIONS_COUNTER_NAME = "time_to_live_operations"
INGEST_HOOKS_EXECUTED_COUNTER_NAME = "ingest_hooks_executed"
BUFFERED_RECORDS_COUNTER_NAME = "buffered_records"

RECORDS_COUNTER_DESCRIPTION = "Number of records processed"
NON_FATAL_ERRORS_COUNTER_DESCRIPTION = "Number of non-fatal errors"
NODES_UPSERTED_COUNTER_DESCRIPTION = "Number of nodes upserted to the graph"
RELS_UPSERTED_COUNTER_DESCRIPTION = "Number of relationships upserted to the graph"
TTL_OPERATIONS_COUNTER_DESCRIPTION = "Number of time-to-live operations executed"
INGEST_HOOKS_EXECUTED_COUNTER_DESCRIPTION = (
    "Number of ingest hooks executed to the graph"
)
STEPS_RUNNING_COUNTER_DESCRIPTION = "Number of steps currently running in the pipeline"
BUFFERED_RECORDS_DESCRIPTION = "Number of records buffered between steps"


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
            self.instruments_by_metric = {
                Metric.RECORDS: Gauge(
                    name=RECORDS_COUNTER_NAME,
                    documentation=RECORDS_COUNTER_DESCRIPTION,
                ),
                Metric.NON_FATAL_ERRORS: Gauge(
                    name=NON_FATAL_ERRORS_COUNTER_NAME,
                    documentation=NON_FATAL_ERRORS_COUNTER_DESCRIPTION,
                ),
                Metric.NODES_UPSERTED: Gauge(
                    name=NODES_UPSERTED_COUNTER_NAME,
                    documentation=NODES_UPSERTED_COUNTER_DESCRIPTION,
                ),
                Metric.RELATIONSHIPS_UPSERTED: Gauge(
                    name=RELS_UPSERTED_COUNTER_NAME,
                    documentation=RELS_UPSERTED_COUNTER_DESCRIPTION,
                ),
                Metric.TIME_TO_LIVE_OPERATIONS: Gauge(
                    name=TTL_OPERATIONS_COUNTER_NAME,
                    documentation=TTL_OPERATIONS_COUNTER_DESCRIPTION,
                ),
                Metric.INGEST_HOOKS_EXECUTED: Gauge(
                    name=INGEST_HOOKS_EXECUTED_COUNTER_NAME,
                    documentation=INGEST_HOOKS_EXECUTED_COUNTER_DESCRIPTION,
                ),
                Metric.STEPS_RUNNING: Gauge(
                    name="steps_running",
                    documentation=STEPS_RUNNING_COUNTER_DESCRIPTION,
                ),
                Metric.BUFFERED_RECORDS: Gauge(
                    name=BUFFERED_RECORDS_COUNTER_NAME,
                    documentation=BUFFERED_RECORDS_DESCRIPTION,
                ),
            }

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

        def increment(self, metric: Metric, value: Number):
            self.instruments_by_metric[metric].inc(value)

        def decrement(self, metric: Metric, value: Number):
            self.instruments_by_metric[metric].dec(value)

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


STATS_TABLE_COLS = ["Statistic", "Value"]


class ConsoleMetricHandler(MetricHandler):
    """A metric handler that prints metrics to the console."""

    def __init__(self, command: Command):
        self.metrics = {x: 0 for x in Metric}
        self.command = command

    def increment(self, metric: Metric, value: Number):
        self.metrics[metric] += value

    def decrement(self, metric: Metric, value: Number):
        self.metrics[metric] -= value

    def stop(self):
        stats = ((k.name, str(v)) for k, v in self.metrics.items() if v > 0)
        table = self.command.table(STATS_TABLE_COLS, stats)
        table.render()


class JsonLogMetricHandler(MetricHandler):
    """A metric handler that logs metrics in JSON format."""

    def __init__(self):
        self.metrics = {x: 0 for x in Metric}
        self.logger = getLogger(__name__)

    def increment(self, metric: Metric, value: Number):
        self.metrics[metric] += value

    def decrement(self, metric: Metric, value: Number):
        self.metrics[metric] -= value

    def stop(self):
        self.logger.info(
            "Metrics Report", extra={k.name: v for k, v in self.metrics.items()}
        )


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
        self.handler.increment(metric, value)

    def decrement(self, metric: Metric, value: Number = 1):
        self.handler.decrement(metric, value)

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
