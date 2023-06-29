from typing import Any, Iterable

from nodestream.model import ProviderContext
from nodestream.value_providers import ValueProvider


class StubbedValueProvider(ValueProvider):
    def __init__(self, values) -> None:
        self.values = values

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        return self.values

    def single_value(self, context: ProviderContext) -> Any:
        return self.values[0]
