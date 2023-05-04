from abc import ABC, abstractmethod
from typing import Any

from ..subclass_registry import SubclassRegistry


NORMALIZER_REGISTRY = SubclassRegistry()


@NORMALIZER_REGISTRY.connect_baseclass
class Normalizer(ABC):
    """A `Normalizer` is responsible for turning objects into a consistent form.

    When data is extracted from pipeline records from a value provider, the `Normalizer`
    is responsible for "cleaning up" the raw data such that is consistent. Often this comes
    in with regard to strings.
    """

    @classmethod
    def arugment_flag(cls):
        return f"do_{NORMALIZER_REGISTRY.name_for(cls)}"

    @abstractmethod
    def normalize_value(self, value: Any) -> Any:
        ...
