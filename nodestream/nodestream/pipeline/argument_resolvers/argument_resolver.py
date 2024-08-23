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

    @staticmethod
    def resolve_argument(value):
        return value

    @classmethod
    def resolve_argument_with_alias(cls, tag, value):
        ArgumentResolver.import_all()
        resolver = ARGUMENT_RESOLVER_REGISTRY.get(tag)
        return resolver.resolve_argument(value)

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        def construct_argument(loader, node):
            return cls.resolve_argument(loader.construct_scalar(node))

        yaml_tag = f"!{ARGUMENT_RESOLVER_REGISTRY.name_for(cls)}"
        loader.add_constructor(yaml_tag, construct_argument)
