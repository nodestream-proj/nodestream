from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Dict, Iterable, Union

from .interpreter_context import InterpreterContext
from ..normalizers import Normalizer

StaticValueOrValueProvider = Union[Any, "ValueProvider"]


class ValueProvider(ABC):
    @classmethod
    def garuntee_value_provider(
        cls, maybe_provider: StaticValueOrValueProvider
    ) -> "ValueProvider":
        return (
            maybe_provider
            if isinstance(maybe_provider, ValueProvider)
            else StaticValueProvider(maybe_provider)
        )

    @classmethod
    def garuntee_provider_dictionary(
        cls, maybe_providers: Dict[Any, StaticValueOrValueProvider]
    ):
        return {k: cls.garuntee_value_provider(v) for k, v in maybe_providers.items()}

    # TODO: Change this API To take a loader that we give it.
    @classmethod
    def install_yaml_tag_constructor(cls):
        pass

    @abstractmethod
    def single_value(self, context: InterpreterContext) -> Any:
        raise NotImplementedError

    @abstractmethod
    def many_values(self, context: InterpreterContext) -> Iterable[Any]:
        raise NotImplementedError

    def normalize(self, value, **args):
        return Normalizer.normalize_by_args(value, **args)

    def normalize_single_value(
        self, context: InterpreterContext, **normalization_args
    ) -> Any:
        return self.normalize(self.single_value(context), **normalization_args)

    def normalize_many_values(
        self, context: InterpreterContext, **normalization_args
    ) -> Iterable[Any]:
        for value in self.many_values(context):
            yield self.normalize(value, **normalization_args)

    @property
    def is_static(self) -> bool:
        return False


class StaticValueProvider(ValueProvider):
    def __init__(self, value) -> None:
        self.value = value

    def single_value(self, _: InterpreterContext) -> Any:
        return self.value

    def many_values(self, _: InterpreterContext) -> Iterable[Any]:
        return self.value if isinstance(self.value, Iterable) else [self.value]

    @property
    def is_static(self) -> bool:
        return True
