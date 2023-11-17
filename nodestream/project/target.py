from dataclasses import dataclass
from typing import Any, Dict


@dataclass(slots=True, frozen=True)
class Target:
    name: str
    connector_config: Dict[str, Any]

    @property
    def connector(self):
        from ..databases import DatabaseConnector

        return DatabaseConnector.from_database_args(**self.connector_config)

    def make_writer(self, **writer_args):
        from ..databases import GraphDatabaseWriter

        return GraphDatabaseWriter.from_file_data(
            **writer_args, **self.connector_config
        )

    def make_type_retriever(self):
        return self.connector.make_type_retriever()
