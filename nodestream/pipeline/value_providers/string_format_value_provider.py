from typing import Any, Dict, Iterable, Type

from yaml import SafeLoader

from .context import ProviderContext
from .value_provider import StaticValueOrValueProvider, ValueProvider


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
        if (fmt := self.fmt.single_value(context)) is None:
            return None

        subs = {
            field: provider.single_value(context)
            for field, provider in self.subs.items()
        }

        return fmt.format(**subs)

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        value = self.single_value(context)
        return [value] if value else []
