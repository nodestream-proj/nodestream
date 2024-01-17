from typing import Any, Iterable, Type

from yaml import SafeLoader

from ..normalizers import Normalizer
from .context import ProviderContext
from .value_provider import ValueProvider


class NormalizerValueProvider(ValueProvider):
    def __init__(self, using: str, data: ValueProvider):
        self.normalizer = Normalizer.from_alias(using)
        self.data = data

    def single_value(self, context: ProviderContext) -> Any:
        return next(self.many_values(context))

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        return map(self.normalizer.normalize_value, self.data.many_values(context))

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!normalize", lambda loader, node: cls(**loader.construct_mapping(node))
        )
