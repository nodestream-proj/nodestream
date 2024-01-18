from dataclasses import dataclass, field
from typing import Any, Dict, List, Set
from importlib import resources

from schema import Or

from nodestream.project.pipeline_definition import (
    PipelineConfiguration,
    PipelineDefinition,
)
from ..file_io import LoadsFromYamlFile
from ..pipeline.scope_config import ScopeConfig


@dataclass
class PluginConfiguration(LoadsFromYamlFile):
    """A `PluginConfiguration` represents a collection of configuration for a plugin.

    A config is a key value pair object in a nodestream scope including plugins.
    It contains a collection of configuration key value pairs to be used by the pipelines of a scope.
    """

    name: str
    pipelines: List[PipelineDefinition]
    pipelines_by_name: Dict[str, PipelineDefinition] = field(default_factory=dict)
    config: ScopeConfig = None
    targets: Set[str] = field(default_factory=set)
    annotations: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                "name": str,
                Optional("config"): ScopeConfig.describe_yaml_schema(),
                Optional("targets"): [str],
                Optional("annotations"): {str: Or(str, int, float, bool)},
                Optional("pipelines"): {
                    str: PipelineConfiguration.describe_yaml_schema()
                },
            }
        )

    @classmethod
    def from_file_data(cls, data) -> "PluginConfiguration":
        name = data.pop("name")
        targets = data.pop("targets", [])
        config = data.pop("config")
        annotations = data.pop("annotations", {})
        pipelines_data = data.pop("pipelines", {})
        pipelines = [
            PipelineDefinition.from_plugin_data(
                {"name": name, **pipeline}, set(targets), annotations
            )
            for name, pipeline in pipelines_data.items()
        ]
        pipelines_by_name = {
            name: PipelineDefinition.from_plugin_data(
                {"name": name, **pipeline}, set(targets), annotations
            )
            for name, pipeline in pipelines_data.items()
        }
        return cls(name, pipelines, pipelines_by_name, config, targets, annotations)

    def to_file_data(self):
        return {
            "name": self.name,
            "pipelines": [
                pipeline.to_plugin_file_data() for pipeline in self.pipelines
            ],
            "targets": self.targets,
            "annotations": self.annotations,
            "config": self.config,
        }

    def get_config_value(self, key):
        return self.config.get_config_value(key)

    def merge_pipeline_data(self, other: "PluginConfiguration"):
        for name, pipeline in self.pipelines_by_name.items():
            other_pipeline = other.pipelines_by_name.get(name)
            pipeline.configuration.targets = set(other.targets)
            pipeline.configuration.annotations = other.annotations
            if other_pipeline is not None:
                pipeline.configuration.merge_with(other_pipeline.configuration)

    @classmethod
    def from_resources(
        cls, name: str, package: resources.Package
    ) -> "PluginConfiguration":
        """Load a `PipelineScope` from a package's resources.

        Each `.yaml` file in the package's resources will be loaded as a pipeline.
        Internally, this uses `importlib.resources` to load the files and calls
        `PipelineDefinition.from_path` on each file. This means that the name of
        the pipeline will be the name of the file without the .yaml extension.

        Args:
            name: The name of the scope.
            package: The name of the package to load from.
            persist: Whether or not to save the scope when the project is saved.

        Returns:
            A `PipelineScope` instance with the pipelines defined in the package.
        """
        pipelines = [
            PipelineDefinition.from_path(f)
            for f in resources.files(package).iterdir()
            if f.suffix == ".yaml"
        ]
        pipelines_by_name = {pipeline.name: pipeline for pipeline in pipelines}
        return cls(name, pipelines, pipelines_by_name)
