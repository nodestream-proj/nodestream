from typing import Any, Type

from yaml import SafeLoader, Node

from .argument_resolver import ArgumentResolver


class RefreshableArgument:
    def __init__(self, loader: SafeLoader, tag: str, val: str) -> None:
        self.loader = loader
        self.tag = tag
        self.val = val

    def get_current_value(self) -> Any:
        node = Node(self.tag, self.val)
        return self.loader.construct_scalar(node)


class RefreshableArgumentResolver(ArgumentResolver):
    """An `ArgumentResolver` that can get a value when called in the future."""

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!refreshable",
            lambda _, node: RefreshableArgument(loader, node.tag, node.value),
        )
