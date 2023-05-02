from abc import ABC, abstractmethod
from typing import Any,  Iterable

from .model import IngestContext

# TODO: Remodel this file.

class ValueProvider(ABC):
    @classmethod
    def install_tag_constructors(cls):
        # This is a method to make it "Feel like" we are doing work here,
        # but simply the act of importing this file does the trick. Each subclass
        # defines a constructor.
        pass

    @abstractmethod
    def provide_single_value_from_context(self, context: IngestContext) -> Any:
        raise NotImplementedError

    @abstractmethod
    def provide_many_values_from_context(self, context: IngestContext) -> Iterable[Any]:
        raise NotImplementedError


