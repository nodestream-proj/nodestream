import os
from abc import ABC
from typing import Type

from yaml import SafeLoader

from ..subclass_registry import SubclassRegistry

ARGUMENT_RESOLVER_REGISTRY = SubclassRegistry()


@ARGUMENT_RESOLVER_REGISTRY.connect_baseclass
class ArgumentResolver(ABC):
    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        pass


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


class IncludeFileResolver(ArgumentResolver):
    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!include",
            lambda loader, node: cls.include_file(loader.construct_scalar(node)),
        )

    @staticmethod
    def resolve_argument(file_path: str):
        from .pipeline_file_loader import PipelineFileSafeLoader

        return PipelineFileSafeLoader.load_file_by_path(file_path)
