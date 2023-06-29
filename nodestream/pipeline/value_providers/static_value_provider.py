from typing import Any, Iterable

from .context import ProviderContext
from .value_provider import ValueProvider


class StaticValueProvider(ValueProvider):
    """A `ValueProvider` that always returns the same value."""

    __slots__ = ("value",)

    def __init__(self, value) -> None:
        self.value = value

    def single_value(self, _: ProviderContext) -> Any:
        return self.value

    def many_values(self, _: ProviderContext) -> Iterable[Any]:
        return self.value if isinstance(self.value, Iterable) else [self.value]

    @property
    def is_static(self) -> bool:
        return True
