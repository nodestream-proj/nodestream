from typing import Any, Iterable, Type

import jmespath
from jmespath.parser import ParsedResult
from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider, ValueProviderException


class JmespathValueProvider(ValueProvider):
    """A `ValueProvider` that uses JMESPath to extract values from a document."""

    __slots__ = ("compiled_query",)

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!jmespath",
            lambda loader, node: cls.from_string_expression(
                loader.construct_scalar(node)
            ),
        )

    @classmethod
    def from_string_expression(cls, expression: str):
        return cls(jmespath.compile(expression))

    def __init__(self, compiled_query: ParsedResult) -> None:
        self.compiled_query = compiled_query

    def search(self, context: ProviderContext):
        raw_search = self.compiled_query.search(context.document)
        if raw_search is None:
            return
        if isinstance(raw_search, list):
            yield from raw_search
        else:
            yield raw_search

    def single_value(self, context: ProviderContext) -> Any:
        try:
            return next(self.search(context), None)
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        try:
            yield from self.search(context)
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def __str__(self) -> str:
        return (
            f"JmespathValueProvider: { {'expression': self.compiled_query.expression} }"
        )


SafeDumper.add_representer(
    JmespathValueProvider,
    lambda dumper, jmespath: dumper.represent_scalar(
        "!jmespath", jmespath.compiled_query.expression
    ),
)
