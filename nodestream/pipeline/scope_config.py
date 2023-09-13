from dataclasses import dataclass
from typing import Dict

from ..file_io import LoadsFromYamlFile


@dataclass
class ScopeConfig(LoadsFromYamlFile):
    """A `Config` represents a collection of configuration for a scope.

    A config is a key value pair object in a nodestream scope including plugins.
    It contains a collection of configuration key value pairs to be used by the pipelines of a scope.
    """

    config: Dict[str, any]

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Schema

        return Schema(dict)

    @classmethod
    def from_file_data(cls, data) -> "ScopeConfig":
        """Creates a config object from file data.

        The file data should be a dictionary mapping of names with a dictionary of config key value pairs and
        should be validated by the schema returned by `describe_yaml_schema`.

        This method should not be called directly. Instead, use `LoadsFromYamlFile.load_from_yaml_file`.

        Args:
            data (Dict): The file data to create the config from.

        Returns:
            Config: The config created from the file data.
        """

        return cls(data)

    def get_config_value(self, key):
        return self.config.get(key)
