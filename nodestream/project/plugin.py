from dataclasses import dataclass, field
from importlib import resources
from typing import Dict, Optional

from schema import Or

from ..file_io import LoadsFromYamlFile
from ..pipeline.scope_config import ScopeConfig
from .pipeline_definition import PipelineConfiguration, PipelineDefinition
from .pipeline_scope import PipelineScope


@dataclass
class PluginConfiguration(LoadsFromYamlFile):
    """A `PluginConfiguration` represents a collection of configuration for a plugin.

    A config is a key value pair object in a nodestream scope including plugins.
    It contains a collection of configuration key value pairs to be used by the pipelines of a scope.
    """

    name: str
    pipelines_by_name: Dict[str, PipelineDefinition] = field(default_factory=dict)
    config: Optional[ScopeConfig] = None
    pipeline_configuration: PipelineConfiguration = None

    @classmethod
    def describe_yaml_schema(cls):
        from schema import And, Optional, Schema, Use

        return Schema(
            {
                "name": str,
                Optional("config"): ScopeConfig.describe_yaml_schema(),
                Optional("targets"): And(Use(set), {str}),
                Optional("annotations"): {str: Or(str, int, float, bool)},
                Optional("pipelines"): [PipelineDefinition.describe_yaml_schema()],
            }
        )

    @classmethod
    def from_file_data(cls, data) -> "PluginConfiguration":
        name = data.pop("name")
        config = data.pop("config")
        pipelines_data = data.pop("pipelines", [])
        configuration = PipelineConfiguration.from_file_data(data)
        pipelines_by_name = {
            pipeline["name"]: PipelineDefinition.from_plugin_data(
                pipeline, configuration
            )
            for pipeline in pipelines_data
        }
        return cls(
            name,
            pipelines_by_name,
            ScopeConfig.from_file_data(config) if config else None,
            configuration,
        )

    def to_file_data(self):
        data = {"name": self.name}

        if self.config is not None:
            data["config"] = self.config.to_file_data()

        if annotations := self.pipeline_configuration.annotations:
            data["annotations"] = annotations

        if targets := self.pipeline_configuration.targets:
            data["targets"] = list(targets)

        if self.pipelines_by_name:
            data["pipelines"] = [
                ppl.to_plugin_file_data() for ppl in self.pipelines_by_name.values()
            ]

        return data

    def update_configurations(self, other: "PluginConfiguration"):
        """Updates the `PluginConfiguration` using the
        PluginConfiguration from the provided PluginConfiguration object.

        Used for adding project plugin configurations to plugins pipelines loaded from resources.
        """
        self.config = other.config
        for name, pipeline in self.pipelines_by_name.items():
            # set the plugin configuration level targets and annotations
            pipeline.configuration.parent = other.pipeline_configuration

            # override the pipeline configuration if they are available
            other_pipeline = other.pipelines_by_name.get(name)
            if other_pipeline is not None:
                pipeline.use_configuration(other_pipeline.configuration)

    def make_scope(self) -> PipelineScope:
        """Creates a `PipelineScope` object from the `PluginConfiguration` object"""
        return PipelineScope(self.name, self.pipelines_by_name, False, self.config)

    @classmethod
    def from_resources(
        cls, name: str, package: resources.Package
    ) -> "PluginConfiguration":
        """Load a `PluginConfiguration` from a package's resources.

        Each `.yaml` file in the package's resources will be loaded as a pipeline.
        Internally, this uses `importlib.resources` to load the files and calls
        `PipelineDefinition.from_path` on each file. This means that the name of
        the pipeline will be the name of the file without the .yaml extension.

        Args:
            name: The name of the scope.
            package: The name of the package to load from.

        Returns:
            A `PluginConfiguration` instance with the pipelines defined in the package.
        """
        pipelines = [
            PipelineDefinition.from_path(f)
            for f in resources.files(package).iterdir()
            if f.suffix == ".yaml"
        ]
        pipelines_by_name = {pipeline.name: pipeline for pipeline in pipelines}
        return cls(name, pipelines_by_name)
