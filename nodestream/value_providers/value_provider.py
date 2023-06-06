from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Type, Union

from yaml import SafeLoader

from ..model import InterpreterContext
from ..normalizers import Normalizer
from ..subclass_registry import SubclassRegistry

StaticValueOrValueProvider = Union[Any, "ValueProvider"]


VALUE_PROVIDER_REGISTRY = SubclassRegistry()


@VALUE_PROVIDER_REGISTRY.connect_baseclass
class ValueProvider(ABC):
    """A `ValueProvider` is a class that can extract values from a document."""

    @classmethod
    def guarantee_value_provider(
        cls, maybe_provider: StaticValueOrValueProvider
    ) -> "ValueProvider":
        from .static_value_provider import StaticValueProvider

        return (
            maybe_provider
            if isinstance(maybe_provider, ValueProvider)
            else StaticValueProvider(maybe_provider)
        )

    @classmethod
    def guarantee_provider_dictionary(
        cls, maybe_providers: Dict[Any, StaticValueOrValueProvider]
    ):
        return {k: cls.guarantee_value_provider(v) for k, v in maybe_providers.items()}

    @classmethod
    def guarantee_provider_list(
        cls, maybe_providers: Iterable[StaticValueOrValueProvider]
    ):
        return [cls.guarantee_value_provider(v) for v in maybe_providers]

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
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
