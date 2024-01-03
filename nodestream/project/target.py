from dataclasses import dataclass
from typing import Any, Dict

from ..file_io import LazyLoadedArgument


@dataclass(slots=True, frozen=True)
class Target:
    name: str
    connector_config: Dict[str, Any]

    @property
    def resolved_connector_config(self):
        return LazyLoadedArgument.resolve_if_needed(self.connector_config)

    @property
    def connector(self):
        from ..databases import DatabaseConnector

        return DatabaseConnector.from_database_args(**self.resolved_connector_config)

    def make_writer(self, **writer_args):
        from ..databases import GraphDatabaseWriter

        return GraphDatabaseWriter.from_file_data(
            **writer_args, **self.resolved_connector_config
        )

    def make_type_retriever(self):
        return self.connector.make_type_retriever()

    def to_file_data(self):
        return self.connector_config
