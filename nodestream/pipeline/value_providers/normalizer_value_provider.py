from typing import Any, Iterable, Type

from yaml import SafeLoader

from ..normalizers import Normalizer
from .context import ProviderContext
from .value_provider import ValueProvider, ValueProviderException


class NormalizerValueProvider(ValueProvider):
    def __init__(self, using: str, data: ValueProvider):
        self.using = using
        self.normalizer = Normalizer.from_alias(using)
        self.data = data

    def single_value(self, context: ProviderContext) -> Any:
        return next(self.many_values(context), None)

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        try:
            yield from map(self.normalizer.normalize, self.data.many_values(context))
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!normalize", lambda loader, node: cls(**loader.construct_mapping(node))
        )

    def __str__(self):
        return f"NormalizerValueProvider: { {'using': self.using, 'data': str(self.data) } }"
