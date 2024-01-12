import os.path
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Set

from ..file_io import LoadsFromYaml, SavesToYaml
from ..pipeline import Pipeline, PipelineFile, PipelineInitializationArguments
from ..schema.schema import IntrospectiveIngestionComponent


def get_default_name(file_path: Path) -> str:
    return file_path.stem


@dataclass
class PipelineConfiguration:
    targets: Set[str] = field(default_factory=list)
    exclude_inherited_targets: bool = False
    annotations: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Or, Schema

        return Schema(
            {
                Optional("annotations"): {str: Or(str, int, float, bool)},
                Optional("targets"): [str],
                Optional("exclude_inherited_targets"): bool,
            },
        )

    @classmethod
    def from_file_data(cls, data):
        annotations = data.pop("annotations", {})
        targets = data.pop("targets", [])
        exclude_targets = data.pop("exclude_inherited_targets", False)
        return cls(set(targets), exclude_targets, annotations)

    def merge_with(self, other: "PipelineConfiguration"):
        self.targets = self.targets | other.targets
        self.exclude_inherited_targets = other.exclude_inherited_targets
        self.annotations.update(other.annotations)

    def add_targets(self, other_targets: Set):
        self.targets = other_targets | self.targets


@dataclass
class PipelineDefinition(IntrospectiveIngestionComponent, SavesToYaml, LoadsFromYaml):
    """A `PipelineDefinition` represents a pipeline that can be loaded from a file.

    `PipelineDefinition` objects are used to load pipelines from files. They themselves
    are not pipelines, but rather represent the metadata needed to load a pipeline from
    a file.

    `PipelineDefinition` objects are also `IntrospectiveIngestionComponent` objects,
    meaning that they can be introspected to determine their known schema definitions.

    `PipelineDefinition` objects are also `SavesToYaml` and `LoadsFromYaml` objects,
    meaning that they can be serialized to and deserialized from YAML data.
    """

    name: str
    file_path: Path
    configuration: PipelineConfiguration = None

    @classmethod
    def from_path(cls, file_path: Path):
        """Create a `PipelineDefinition` from a file path.

        The name of the pipeline will be the stem of the file path, and the annotations
        will be empty. The pipeline will be loaded from the provided file path.

        Args:
            file_path (Path): The path to the file from which to load the pipeline.

        Returns:
            PipelineDefinition: The `PipelineDefinition` object.
        """
        return cls(get_default_name(file_path), file_path, None)

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Or, Schema

        return Schema(
            Or(
                str,
                {
                    Optional("name"): str,
                    "path": os.path.exists,
                    Optional("annotations"): {str: Or(str, int, float, bool)},
                    Optional("targets"): [str],
                    Optional("exclude_inherited_targets"): bool,
                },
            )
        )

    @classmethod
    def from_file_data(cls, data, parent_annotations) -> "PipelineDefinition":
        if isinstance(data, str):
            data = {"path": data}

        file_path = Path(data.pop("path"))
        name = data.pop("name", get_default_name(file_path))
        exclude_targets = data.pop("exclude_inherited_targets", False)
        annotations = data.pop("annotations", {})
        targets = data.pop("targets", [])
        return cls(
            name,
            file_path,
            PipelineConfiguration(
                set(targets), exclude_targets, {**parent_annotations, **annotations}
            ),
        )

    def to_file_data(self, verbose: bool = False):
        using_default_name = self.name == self.file_path.stem
        if (
            using_default_name
            and not self.get_annotations_from_config()
            and not verbose
        ):
            return str(self.file_path)

        result = {"path": str(self.file_path)}
        if not using_default_name or verbose:
            result["name"] = self.name
        annotations = self.get_annotations_from_config()
        targets = self.get_targets_from_config()
        if targets or verbose:
            result["targets"] = targets
        if annotations or verbose:
            result["annotations"] = annotations

        return result

    def get_annotations_from_config(self):
        annotations = {}
        if self.configuration:
            annotations.update(self.configuration.annotations)
        return annotations

    def get_targets_from_config(self):
        targets = set()
        if self.configuration:
            targets = self.configuration.targets
        return targets

    def excluded_inherited_targets(self):
        excluded = False
        if self.configuration:
            excluded = self.configuration.exclude_inherited_targets
        return excluded

    def initialize(self, init_args: PipelineInitializationArguments) -> Pipeline:
        return PipelineFile(self.file_path).load_pipeline(init_args)

    def remove_file(self, missing_ok: bool = True):
        """Remove the file associated with this `PipelineDefinition`.

        Args:
            missing_ok (bool, optional): Whether to ignore missing files. Defaults to True.

        Raises:
            FileNotFoundError: If the file does not exist and `missing_ok` is False.
        """
        self.file_path.unlink(missing_ok=missing_ok)

    def initialize_for_introspection(self) -> Pipeline:
        return self.initialize(PipelineInitializationArguments.for_introspection())

    def gather_object_shapes(self):
        return self.initialize_for_introspection().gather_object_shapes()

    def gather_present_relationships(self):
        return self.initialize_for_introspection().gather_present_relationships()

    def gather_used_indexes(self):
        return self.initialize_for_introspection().gather_used_indexes()
