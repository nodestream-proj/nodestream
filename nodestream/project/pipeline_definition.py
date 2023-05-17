from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from ..pipeline import Pipeline, PipelineFileLoader, PipelineInitializationArguments


def get_default_name(file_path: Path) -> str:
    return file_path.stem


@dataclass
class PipelineDefinition:
    """A `PipelineDefinition` represents a pipeline that can be loaded from a file."""

    name: str
    file_path: Path
    annotations: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_file_data(cls, data, parent_annotations):
        if isinstance(data, str):
            data = {"path": data}

        file_path = Path(data.pop("path"))
        name = data.pop("name", get_default_name(file_path))
        annotations = data.pop("annotations", {})
        return cls(name, file_path, {**parent_annotations, **annotations})

    def initialize(self, init_args: PipelineInitializationArguments) -> Pipeline:
        return PipelineFileLoader(self.file_path).load_pipeline(init_args)
