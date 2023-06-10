from typing import Any, Iterable, Type

from yaml import SafeLoader

from ..model import InterpreterContext
from .value_provider import ValueProvider


class VariableValueProvider(ValueProvider):
    """A `ValueProvider` that uses a variable to extract values from a document."""

    __slots__ = ("variable_name",)

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!variable",
            lambda loader, node: cls(loader.construct_scalar(node)),
        )

    def __init__(self, variable_name: str) -> None:
        self.variable_name = variable_name

    def single_value(self, context: InterpreterContext) -> Any:
        return context.variables.get(self.variable_name)

    def many_values(self, context: InterpreterContext) -> Iterable[Any]:
        value = self.single_value(context)
        if value is None:
            return []
        return value if isinstance(value, list) else [value]
