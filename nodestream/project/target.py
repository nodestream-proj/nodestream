import logging
from dataclasses import dataclass, field
from typing import Any, Dict

from ..file_io import LazyLoadedArgument
from ..schema.migrations import Migrator

logger = logging.getLogger(__name__)

WRITER_ARGUMENTS = (
    "batch_size",
    "collect_stats",
    "ingest_strategy_name",
    "flush_concurrency",
)


@dataclass(slots=True, frozen=True)
class Target:
    name: str
    connector_config: Dict[str, Any]
    writer_arguments: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_file_data(cls, name: str, file_data):
        writer_arguments = {
            item: value
            for item in WRITER_ARGUMENTS
            if (value := file_data.pop(item, None)) is not None
        }

        return cls(
            name=name,
            connector_config=file_data,
            writer_arguments=writer_arguments,
        )

    @property
    def resolved_connector_config(self):
        return LazyLoadedArgument.resolve_if_needed(self.connector_config)

    @property
    def connector(self):
        return self.make_connector()

    def make_connector(self, **overrides):
        from ..databases import DatabaseConnector

        config = {**self.resolved_connector_config, **overrides}
        logger.info(
            "Effective connector config for target '%s': %s",
            self.name,
            config,
        )
        return DatabaseConnector.from_database_args(**config)

    def make_writer(self, connector_overrides=None, **writer_overrides):
        from ..databases import GraphDatabaseWriter

        writer_args = {**self.writer_arguments, **writer_overrides}
        connector = self.make_connector(**(connector_overrides or {}))
        return GraphDatabaseWriter.from_connector(connector=connector, **writer_args)

    def make_type_retriever(self, limit: int = 1000):
        return self.connector.make_type_retriever(limit=limit)

    def make_migrator(self) -> "Migrator":
        return self.connector.make_migrator()

    def to_file_data(self):
        return dict(**self.connector_config, **self.writer_arguments)
