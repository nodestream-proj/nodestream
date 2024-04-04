from dataclasses import dataclass, field
from typing import Any, Dict

from ..file_io import LazyLoadedArgument
from ..schema.migrations import Migrator

WRITER_ARGUMENTS = ("batch_size", "collect_stats", "ingest_strategy_name")


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
        from ..databases import DatabaseConnector

        return DatabaseConnector.from_database_args(**self.resolved_connector_config)

    def make_writer(self):
        from ..databases import GraphDatabaseWriter

        return GraphDatabaseWriter.from_connector(
            connector=self.connector, **self.writer_arguments
        )

    def make_type_retriever(self):
        return self.connector.make_type_retriever()

    def make_migrator(self) -> "Migrator":
        return self.connector.make_migrator()

    def to_file_data(self):
        return dict(**self.connector_config, **self.writer_arguments)
