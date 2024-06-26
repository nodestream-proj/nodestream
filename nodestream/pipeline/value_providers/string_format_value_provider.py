from typing import Any, Dict, Iterable, Type

from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import (
    StaticValueOrValueProvider,
    ValueProvider,
    ValueProviderException,
)


class StringFormattingValueProvider(ValueProvider):
    """A `ValueProvider` that uses string formatting to produce a value."""

    __slots__ = ("fmt", "subs")

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!format", lambda loader, node: cls(**loader.construct_mapping(node))
        )

    def __init__(
        self,
        fmt: StaticValueOrValueProvider,
        **subs: Dict[str, StaticValueOrValueProvider],
    ) -> None:
        self.fmt = ValueProvider.guarantee_value_provider(fmt)
        self.subs = ValueProvider.guarantee_provider_dictionary(subs)

    def single_value(self, context: ProviderContext) -> Any:
        try:
            if (fmt := self.fmt.single_value(context)) is None:
                return None

            subs = {
                field: provider.single_value(context)
                for field, provider in self.subs.items()
            }

            if not all(value is not None for value in subs.values()):
                return None

            return fmt.format(**subs)
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        value = self.single_value(context)
        return [value] if value else []

    def __str__(self):
        return f"StringFormattingValueProvider: { {'format': str(self.fmt), 'subs': str(self.subs) } }"


SafeDumper.add_representer(
    StringFormattingValueProvider,
    lambda dumper, mapping: dumper.represent_mapping(
        "!format", dict(fmt=mapping.fmt, **mapping.subs)
    ),
)
