from typing import Any, Iterable, Type

import jmespath
from jmespath.parser import ParsedResult
from yaml import SafeLoader

from ..model import InterpreterContext
from .value_provider import ValueProvider


class JmespathValueProvider(ValueProvider):
    """A `ValueProvider` that uses JMESPath to extract values from a document."""

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!jmespath",
            lambda loader, node: cls(jmespath.compile(loader.construct_scalar(node))),
        )

    def __init__(self, compiled_query: ParsedResult) -> None:
        self.compiled_query = compiled_query

    def search(self, context: InterpreterContext):
        raw_search = self.compiled_query.search(context.document)
        if raw_search is None:
            return
        if isinstance(raw_search, list):
            yield from raw_search
        else:
            yield raw_search

    def single_value(self, context: InterpreterContext) -> Any:
        return next(self.search(context), None)

    def many_values(self, context: InterpreterContext) -> Iterable[Any]:
        return list(self.search(context))
