from typing import Any, Iterable, Type

import jq
from yaml import SafeLoader

from ..model import InterpreterContext
from .value_provider import ValueProvider


class JqValueProvider(ValueProvider):
    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!jq", lambda loader, node: cls(jq.compile(loader.construct_scalar(node)))
        )

    def __init__(self, jq_program) -> None:
        self.jq_program = jq_program

    def single_value(self, context: InterpreterContext) -> Any:
        return self.jq_program.input(context.document).first()

    def many_values(self, context: InterpreterContext) -> Iterable[Any]:
        return self.jq_program.input(context.document).iter()
