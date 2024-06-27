from typing import Any, Iterable, Type

from yaml import SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider, ValueProviderException


class SplitValueProvider(ValueProvider):
    def __init__(self, delimiter: str, data: ValueProvider):
        self.delimiter = delimiter
        self.data = data

    def single_value(self, context: ProviderContext) -> Any:
        return next(self.many_values(context))

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        try:
            for value in self.data.many_values(context):
                if not isinstance(value, str):
                    raise TypeError(
                        f"Expected {self.data} to yield strings, but got {value!r}"
                    )
                yield from value.split(self.delimiter)
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!split", lambda loader, node: cls(**loader.construct_mapping(node))
        )

    def __str__(self):
        return f"SplitValueProvider: { {'delimiter': self.delimiter, 'data': str(self.data) } }"
