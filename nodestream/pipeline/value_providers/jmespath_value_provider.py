from typing import Any, Iterable, Type

import jmespath
from jmespath.parser import ParsedResult
from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider


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
        return next(self.search(context), None)

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        return self.search(context)


# NOTE: This is here because the default pipeline generation includes a jmespath.
# So we needed a way to represent this. If this becomes more of a thing, we
# should consider doing something more robust
SafeDumper.add_representer(
    JmespathValueProvider,
    lambda dumper, jmespath: dumper.represent_scalar(
        "!jmespath", jmespath.compiled_query.expression
    ),
)
