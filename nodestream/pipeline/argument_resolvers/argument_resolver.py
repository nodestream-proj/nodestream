from abc import ABC
from typing import Type

from yaml import SafeLoader

from ...pluggable import Pluggable
from ...subclass_registry import SubclassRegistry

ARGUMENT_RESOLVER_REGISTRY = SubclassRegistry()


@ARGUMENT_RESOLVER_REGISTRY.connect_baseclass
class ArgumentResolver(Pluggable, ABC):
    """An `ArgumentResolver` is a class that can resolve a value by injecting into the yaml parser."""

    entrypoint_name = "argument_resolvers"

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        pass
