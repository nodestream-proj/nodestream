from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from ..model import IntrospectiveIngestionComponent
from ..pipeline import Pipeline, PipelineFileLoader, PipelineInitializationArguments


def get_default_name(file_path: Path) -> str:
    return file_path.stem


@dataclass
class PipelineDefinition(IntrospectiveIngestionComponent):
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

    @classmethod
    def from_path(cls, path: Path):
        return cls(name=get_default_name(path), file_path=path)

    def as_file_definition(self):
        using_default_name = self.name == self.file_path.stem
        if using_default_name and not self.annotations:
            return str(self.file_path)

        result = {"path": str(self.file_path)}
        if not using_default_name:
            result["name"] = self.name
        if self.annotations:
            result["annotations"] = self.annotations

        return result

    def remove_file(self, missing_ok: bool = True):
        self.file_path.unlink(missing_ok=missing_ok)

    def intialize_for_introspection(self) -> Pipeline:
        return self.initialize(PipelineInitializationArguments.for_introspection())

    def gather_object_shapes(self):
        return self.intialize_for_introspection().gather_object_shapes()

    def gather_present_relationships(self):
        return self.intialize_for_introspection().gather_present_relationships()

    def gather_used_indexes(self):
        return self.intialize_for_introspection().gather_used_indexes()
