from abc import ABC, abstractmethod

from ...pipeline.value_providers import ProviderContext
from ...pluggable import Pluggable
from ...schema.schema import IntrospectiveIngestionComponent
from ...subclass_registry import SubclassRegistry

INTERPRETATION_REGISTRY = SubclassRegistry()


@INTERPRETATION_REGISTRY.connect_baseclass
class Interpretation(IntrospectiveIngestionComponent, Pluggable, ABC):
    entrypoint_name = "interpretations"

    @abstractmethod
    def interpret(self, context: ProviderContext):
        raise NotImplementedError

    @classmethod
    def from_file_data(cls, **arguments) -> "Interpretation":
        name = arguments.pop("type")
        class_to_load = INTERPRETATION_REGISTRY.get(name)
        return class_to_load(**arguments)

    def gather_used_indexes(self):
        yield from []

    def gather_object_shapes(self):
        yield from []

    def gather_present_relationships(self):
        yield from []
