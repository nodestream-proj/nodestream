import os
from typing import Type

from yaml import SafeLoader

from .arugment_resolver import ArgumentResolver


class EnvironmentResolver(ArgumentResolver):
    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!env",
            lambda loader, node: cls.resolve_argument(loader.construct_scalar(node)),
        )

    @staticmethod
    def resolve_argument(variable_name):
        return os.environ[variable_name]
