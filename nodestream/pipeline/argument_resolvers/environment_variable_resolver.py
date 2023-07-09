import os
from typing import Type

from yaml import SafeLoader

from .argument_resolver import ArgumentResolver


class EnvironmentResolver(ArgumentResolver):
    """An `EnvironmentResolver` is an `ArgumentResolver` that can resolve an environment variable into its value."""

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!env",
            lambda loader, node: cls.resolve_argument(loader.construct_scalar(node)),
        )

    @staticmethod
    def resolve_argument(variable_name):
        return os.environ[variable_name]
