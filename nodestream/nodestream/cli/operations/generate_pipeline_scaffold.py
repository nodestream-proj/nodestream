from pathlib import Path
from typing import Any, Iterable

import yaml

from ...pipeline.value_providers import JmespathValueProvider
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


def pretty_print_yaml_to_file(path: Path, data: Any):
    with open(path, "w") as fp:
        yaml.safe_dump(data, fp, indent=2, sort_keys=True)


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
        pretty_print_yaml_to_file(path, SIMPLE_PIPELINE)
