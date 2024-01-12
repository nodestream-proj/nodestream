from dataclasses import dataclass, field
from typing import Any, Dict, List, Set
from schema import Or

from nodestream.project.pipeline_definition import (
    PipelineConfiguration,
    PipelineDefinition,
)
from nodestream.project.pipeline_scope import PipelineScope

from ..file_io import LoadsFromYamlFile
from ..pipeline.scope_config import ScopeConfig

IS_PLUGIN = True


@dataclass
class PluginConfiguration(LoadsFromYamlFile):
    """A `PluginConfiguration` represents a collection of configuration for a plugin.

    A config is a key value pair object in a nodestream scope including plugins.
    It contains a collection of configuration key value pairs to be used by the pipelines of a scope.
    """

    name: str
    pipelines: Dict[str, PipelineConfiguration] = field(default_factory=dict)
    config: ScopeConfig = None
    targets: Set[str] = field(default_factory=list)
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
        pipelines_data = data.pop("pipelines", {})
        annotations = pipelines_data.pop("annotations", {})
        pipelines_config = {
            name: PipelineConfiguration.from_file_data(config)
            for (name, config) in pipelines_data.items()
        }
        return cls(
            name,
            pipelines_config,
            ScopeConfig.from_file_data(config),
            targets,
            annotations,
        )

    def get_config_value(self, key):
        return self.config.get_config_value(key)

    def configure_scope(self, scope: PipelineScope):
        scope.set_configuration(self.config)
        scope.set_targets(self.targets)
        scope.set_annotations(self.annotations)
        scope.update_pipeline_configurations(self.pipelines)
