from abc import ABC
from typing import Type

from yaml import SafeLoader

from ..subclass_registry import SubclassRegistry

ARGUMENT_RESOLVER_REGISTRY = SubclassRegistry()


@ARGUMENT_RESOLVER_REGISTRY.connect_baseclass
class ArgumentResolver(ABC):
    """An `ArgumentResolver` is a class that can resolve a value by injecting into the yaml parser."""

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        pass
