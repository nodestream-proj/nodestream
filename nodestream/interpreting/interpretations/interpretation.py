from abc import ABC, abstractmethod

from ...pipeline.value_providers import ProviderContext
from ...pluggable import Pluggable
from ...schema import ExpandsSchema
from ...subclass_registry import SubclassRegistry
from .conditions import Condition

INTERPRETATION_REGISTRY = SubclassRegistry()


@INTERPRETATION_REGISTRY.connect_baseclass
class Interpretation(ExpandsSchema, Pluggable, ABC):
    entrypoint_name = "interpretations"
    assigns_source_nodes = False

    @abstractmethod
    def interpret(self, context: ProviderContext):
        raise NotImplementedError

    @classmethod
    def from_file_data(cls, **arguments) -> "Interpretation":
        name = arguments.pop("type")
        condition = arguments.pop("condition", None)
        class_to_load = INTERPRETATION_REGISTRY.get(name)
        interpretation = class_to_load(**arguments)
        if condition:
            condition = Condition.from_tagged_file_data(**condition)
            return ConditionedInterpretation(condition, interpretation)
        return interpretation


class ConditionedInterpretation(Interpretation):
    def __init__(self, condition: Condition, interpretation: Interpretation):
        self.condition = condition
        self.interpretation = interpretation

    def interpret(self, context: ProviderContext):
        if self.condition.evaluate(context):
            self.interpretation.interpret(context)
