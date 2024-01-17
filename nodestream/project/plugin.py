from dataclasses import dataclass
from typing import Set

from ..file_io import LoadsFromYamlFile
from ..pipeline.scope_config import ScopeConfig


@dataclass
class PluginConfiguration(LoadsFromYamlFile):
    """A `PluginScope` represents a collection of configuration for a plugin.

    A config is a key value pair object in a nodestream scope including plugins.
    It contains a collection of configuration key value pairs to be used by the pipelines of a scope.
    """

    name: str
    config: ScopeConfig = None
    targets: Set[str] = None

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
    def from_file_data(cls, data) -> "PluginConfiguration":
        name = data.pop("name")
        targets = data.pop("targets", [])
        config = data.pop("config")
        return cls(name, ScopeConfig.from_file_data(config), targets)

    def get_config_value(self, key):
        return self.config.get_config_value(key)
