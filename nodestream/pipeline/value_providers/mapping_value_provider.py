from typing import Any, Iterable, Type

from yaml import SafeLoader

from .context import ProviderContext
from .value_provider import StaticValueOrValueProvider, ValueProvider


class MappingValueProvider(ValueProvider):
    """A `ValueProvider` that uses a mapping to extract values from a document."""

    __slots__ = ("mapping_name", "key")

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!mapping",
            lambda loader, node: MappingValueProvider(**loader.construct_mapping(node)),
        )

    def __init__(self, mapping_name: str, key: StaticValueOrValueProvider) -> None:
        self.mapping_name = mapping_name
        self.key = ValueProvider.guarantee_value_provider(key)

    def single_value(self, context: ProviderContext) -> Any:
        mapping = context.mappings.get(self.mapping_name)
        if not mapping:
            return

        key = self.key.single_value(context)
        if not key:
            return

        value = ValueProvider.guarantee_value_provider(mapping.get(key))
        return value.single_value(context)

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        value = self.single_value(context)
        return [value] if value else []
