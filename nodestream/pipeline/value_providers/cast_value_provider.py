from decimal import Decimal
from typing import Any, Iterable, Type

from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider, ValueProviderException


class CastValueProvider(ValueProvider):
    CONVERTERS = {
        "int": int,
        "float": float,
        "decimal": Decimal,
        "str": str,
        "bool": bool,
        "bytes": bytes,
    }

    def __init__(self, data: ValueProvider, to: str):
        self.data = data
        self.to = to

    def cast_value(self, value):
        return self.CONVERTERS[self.to](value)

    def single_value(self, context: ProviderContext) -> Any:
        try:
            return self.cast_value(self.data.single_value(context))
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        try:
            return map(self.cast_value, self.data.many_values(context))
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!cast", lambda loader, node: cls(**loader.construct_mapping(node))
        )

    def __str__(self):
        return f"CastValueProvider: { {'data': str(self.data), 'to': str(self.to)} }"


SafeDumper.add_representer(
    CastValueProvider,
    lambda dumper, cast: dumper.represent_mapping(
        "!cast",
        {"data": cast.data, "to": cast.to},
    ),
)
