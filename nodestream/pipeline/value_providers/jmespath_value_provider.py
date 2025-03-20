from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Iterable, Type

import jmespath
from jmespath.parser import ParsedResult
from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider, ValueProviderException


# `QueryStrategy` is here to provide the seam for different optimizations
# for executing jmespath queries. We can either execute a "fully fledged"
# jmespath query or we can implement some simple access patterns that
# are faster to execute. For example, if the expression is a simple key
# lookup, we can just use the key directly instead of compiling the
# jmespath expression and then executing it with all the weight and
# overhead that comes with it.


class QueryStrategy(ABC):
    @classmethod
    def from_string_expression(cls, expression: str):
        if expression.isalpha():
            return KeyLookup(expression)

        compiled_query = jmespath.compile(expression)
        return ExecuteJmespath(compiled_query)

    @abstractmethod
    def search(self, context: ProviderContext):
        pass


class ExecuteJmespath(QueryStrategy):
    def __init__(self, compiled_query: ParsedResult) -> None:
        self.compiled_query = compiled_query

    def search(self, context: ProviderContext):
        return self.compiled_query.search(context.document)

    def __str__(self) -> str:
        return str(self.compiled_query.expression)


class KeyLookup(QueryStrategy):
    def __init__(self, key: str) -> None:
        self.key = key

    def search(self, context: ProviderContext):
        return context.document.get(self.key, None)

    def __str__(self) -> str:
        return self.key


class JmespathValueProvider(ValueProvider):
    """A `ValueProvider` that uses JMESPath to extract values from a document."""

    __slots__ = ("compiled_query",)

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!jmespath",
            lambda loader_param, node: cls.from_string_expression(
                loader_param.construct_scalar(node)
            ),
        )

    @classmethod
    def from_string_expression(cls, expression: str):
        return cls(QueryStrategy.from_string_expression(expression))

    def __init__(self, strategy: QueryStrategy) -> None:
        self.strategy = strategy

    def single_value(self, context: ProviderContext) -> Any:
        try:
            return self.strategy.search(context)
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        try:
            result = self.strategy.search(context)
            if not result:
                return

            if isinstance(result, (bool, str, int, float)):
                yield result
            elif isinstance(result, Sequence):
                yield from result
            elif result:
                yield result

            return
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def __str__(self) -> str:
        return f"JmespathValueProvider: { {'expression': str(self.strategy)} }"


SafeDumper.add_representer(
    JmespathValueProvider,
    lambda dumper, jp: dumper.represent_scalar("!jmespath", str(jp.strategy)),
)
