from typing import Any, Iterable

from nodestream.model import InterpreterContext
from nodestream.value_providers import ValueProvider


class StubbedValueProvider(ValueProvider):
    def __init__(self, values) -> None:
        self.values = values

    def many_values(self, context: InterpreterContext) -> Iterable[Any]:
        return self.values

    def single_value(self, context: InterpreterContext) -> Any:
        return self.values[0]
