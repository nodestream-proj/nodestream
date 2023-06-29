from typing import Any, Iterable, Type

import jq
from yaml import SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider


class JqValueProvider(ValueProvider):
    """A `ValueProvider` that uses Jq to extract values from a document."""

    __slots__ = ("jq_program",)

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!jq", lambda loader, node: cls(jq.compile(loader.construct_scalar(node)))
        )

    def __init__(self, jq_program) -> None:
        self.jq_program = jq_program

    def search(self, context: ProviderContext):
        raw_search = self.jq_program.input(context.document).all()
        for hit in raw_search:
            if hit is None:
                return
            if isinstance(hit, list):
                yield from hit
            else:
                yield hit

    def single_value(self, context: ProviderContext) -> Any:
        return next(self.search(context), None)

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        return self.search(context)
