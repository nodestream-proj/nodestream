from typing import Any, Iterable

from nodestream.interpreters.interpreter_context import InterpreterContext
from nodestream.interpreters.value_providers import ValueProvider


class StubbedValueProvider(ValueProvider):
    def __init__(self, values) -> None:
        self.values = values

    def provide_many_values_from_context(self, context: InterpreterContext) -> Iterable[Any]:
        return self.values

    def provide_single_value_from_context(self, context: InterpreterContext) -> Any:
        return self.values[0]
