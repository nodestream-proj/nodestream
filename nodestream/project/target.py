from dataclasses import dataclass
from typing import Any, Dict


@dataclass(slots=True, frozen=True)
class Target:
    name: str
    connector_config: Dict[str, Any]

    def make_writer(self, **writer_args):
        from ..databases import GraphDatabaseWriter

        return GraphDatabaseWriter.from_file_data(
            **writer_args, **self.connector_config
        )
