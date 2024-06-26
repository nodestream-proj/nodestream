from typing import Any, Iterable

from nodestream.pipeline.value_providers import ProviderContext, ValueProvider


class StubbedValueProvider(ValueProvider):
    def __init__(self, values) -> None:
        self.values = values

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        return self.values

    def single_value(self, context: ProviderContext) -> Any:
        return self.values[0]


class ErrorValueProvider(ValueProvider):
    def __init__(self) -> None:
        pass

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        raise

    def single_value(self, context: ProviderContext) -> Any:
        raise
