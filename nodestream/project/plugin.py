from dataclasses import dataclass
from typing import List

from ..pipeline.scope_config import ScopeConfig
from ..file_io import LazyLoadedArgument, LoadsFromYamlFile


@dataclass
class PluginScope(LoadsFromYamlFile):
    """A `PluginScope` represents a collection of configuration for a plugin.

    A config is a key value pair object in a nodestream scope including plugins.
    It contains a collection of configuration key value pairs to be used by the pipelines of a scope.
    """

    name: str
    config: ScopeConfig = None
    targets: List[str] = None

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                "name": str,
                Optional("config"): ScopeConfig.describe_yaml_schema(),
                Optional("targets"): [str],
            }
        )

    @classmethod
    def from_file_data(cls, data) -> "PluginScope":
        name = data.pop("name")
        targets = data.pop("targets", [])
        config = data.pop("config", {})
        return cls(name, config, targets)

    def get_config_value(self, key):
        return LazyLoadedArgument.resolve_if_needed(self.config.get(key))