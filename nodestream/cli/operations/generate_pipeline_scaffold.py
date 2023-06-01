from pathlib import Path
from typing import Iterable

from ...utilities import pretty_print_yaml_to_file
from ...value_providers import JmespathValueProvider
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

DEFAULT_PIPELINE_FILE_NAME = "sample.yaml"

SIMPLE_PIPELINE = [
    {
        "implementation": "nodestream.pipeline:IterableExtractor",
        "factory": "range",
        "arguments": {"stop": 100000},
    },
    {
        "implementation": "nodestream.interpreting:Interpreter",
        "arguments": {
            "interpretations": [
                {
                    "type": "source_node",
                    "node_type": "Number",
                    "key": {
                        "number": JmespathValueProvider.from_string_expression("index")
                    },
                }
            ]
        },
    },
]

WRITER_CONFIG_BY_DATABASE = {
    "neo4j": {
        "implementation": "nodestream.databases:GraphDatabaseWriter",
        "arguments": {
            "batch_size": 1000,
            "database": "neo4j",
            "uri": "bolt://localhost:7687",
            "username": "neo4j",
            "password": "neo4j123",
        },
    }
}


class GeneratePipelineScaffold(Operation):
    def __init__(
        self,
        project_root: Path,
        database_name: str,
        pipeline_file_name: str = DEFAULT_PIPELINE_FILE_NAME,
    ) -> None:
        self.project_root = project_root
        self.database_name = database_name
        self.pipeline_file_name = pipeline_file_name

    async def perform(self, _: NodestreamCommand) -> Iterable[Path]:
        path = self.prepare_file_path()
        self.make_pipeline_at_path(path)
        return [path]

    def prepare_file_path(self) -> Path:
        pipeline_dir = self.project_root / "pipelines"
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        return pipeline_dir / self.pipeline_file_name

    def make_pipeline_at_path(self, path: Path):
        steps = SIMPLE_PIPELINE.copy()
        steps.append(WRITER_CONFIG_BY_DATABASE[self.database_name])
        pretty_print_yaml_to_file(path, steps)
