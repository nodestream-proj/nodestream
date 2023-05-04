from abc import ABC, abstractmethod

from ..model import InterpreterContext, IntrospectableIngestionComponent
from ..subclass_registry import SubclassRegistry

# TODO: Reintroduce logic for Loading and `Interpretation` from file data.
# TODO: Reintroduce inheriting from Schema Introspectable Component.


INTERPRETATION_REGISTRY = SubclassRegistry()


@INTERPRETATION_REGISTRY.connect_baseclass
class Interpretation(IntrospectableIngestionComponent, ABC):
    @abstractmethod
    def interpret(self, context: InterpreterContext):
        raise NotImplementedError

    def gather_used_indexes(self):
        yield from []

    def gather_object_shapes(self):
        yield from []

    def gather_present_relationships(self):
        yield from []
