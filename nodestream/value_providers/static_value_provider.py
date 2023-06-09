from typing import Any, Iterable

from ..model import InterpreterContext
from .value_provider import ValueProvider


class StaticValueProvider(ValueProvider):
    """A `ValueProvider` that always returns the same value."""

    __slots__ = ("value",)

    def __init__(self, value) -> None:
        self.value = value

    def single_value(self, _: InterpreterContext) -> Any:
        return self.value

    def many_values(self, _: InterpreterContext) -> Iterable[Any]:
        return self.value if isinstance(self.value, Iterable) else [self.value]

    @property
    def is_static(self) -> bool:
        return True
