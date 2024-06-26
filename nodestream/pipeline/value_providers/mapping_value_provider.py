from typing import Any, Iterable, Type

from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import (
    StaticValueOrValueProvider,
    ValueProvider,
    ValueProviderException,
)


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
        try:
            mapping = context.mappings.get(self.mapping_name)
            if not mapping:
                return

            key = self.key.single_value(context)
            if not key:
                return

            value = ValueProvider.guarantee_value_provider(mapping.get(key))
            return value.single_value(context)
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        value = self.single_value(context)
        return [value] if value else []

    def __str__(self) -> str:
        return f"MappingValueProvider: { {'mapping_name': self.mapping_name, 'key': str(self.key)} }"


SafeDumper.add_representer(
    MappingValueProvider,
    lambda dumper, mapping: dumper.represent_mapping(
        "!mapping", {"mapping_name": mapping.mapping_name, "key": mapping.key}
    ),
)
