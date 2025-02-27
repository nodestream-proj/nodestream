from abc import ABC, abstractmethod

from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...subclass_registry import SubclassRegistry

CONDITION_SUBCLASS_REGISTRY = SubclassRegistry()
COMPARISON_OPERATOR_SUBCLASS_REGISTRY = SubclassRegistry()


@CONDITION_SUBCLASS_REGISTRY.connect_baseclass
class Condition(ABC):
    @abstractmethod
    def evaluate(self, context: ProviderContext) -> bool:
        pass

    @classmethod
    @abstractmethod
    def from_file_data(cls, **arguments) -> "Condition":
        pass

    @staticmethod
    def from_tagged_file_data(**data) -> "Condition":
        name = data.pop("type", CONDITION_SUBCLASS_REGISTRY.name_for(AlwaysTrue))
        class_to_load = CONDITION_SUBCLASS_REGISTRY.get(name)
        return class_to_load.from_file_data(**data)


class AlwaysTrue(Condition, alias="true"):
    def evaluate(self, context: ProviderContext) -> bool:
        return True

    @classmethod
    def from_file_data(cls, **arguments) -> "Condition":
        return cls()


class Or(Condition, alias="or"):
    def __init__(self, *conditions: Condition):
        self.conditions = conditions

    def evaluate(self, context: ProviderContext) -> bool:
        return any(condition.evaluate(context) for condition in self.conditions)

    @classmethod
    def from_file_data(cls, **arguments) -> "Condition":
        conditions = [
            Condition.from_tagged_file_data(**condition)
            for condition in arguments["conditions"]
        ]
        return cls(*conditions)


class And(Condition, alias="and"):
    def __init__(self, *conditions: Condition):
        self.conditions = conditions

    def evaluate(self, context: ProviderContext) -> bool:
        return all(condition.evaluate(context) for condition in self.conditions)

    @classmethod
    def from_file_data(cls, **arguments) -> "Condition":
        conditions = [
            Condition.from_tagged_file_data(**condition)
            for condition in arguments["conditions"]
        ]
        return cls(*conditions)


class Not(Condition, alias="not"):
    def __init__(self, condition: Condition):
        self.condition = condition

    def evaluate(self, context: ProviderContext) -> bool:
        return not self.condition.evaluate(context)

    @classmethod
    def from_file_data(cls, **arguments) -> "Condition":
        condition = Condition.from_tagged_file_data(**arguments["condition"])
        return cls(condition)


@COMPARISON_OPERATOR_SUBCLASS_REGISTRY.connect_baseclass
class ComparisonOperator(ABC):
    @abstractmethod
    def operate(self, left, right) -> bool:
        pass


class EqualsOperator(ComparisonOperator, alias="equals"):
    def operate(self, left, right) -> bool:
        return left == right


class GreaterThanOperator(ComparisonOperator, alias="greater_than"):
    def operate(self, left, right) -> bool:
        return left > right


class LessThanOperator(ComparisonOperator, alias="less_than"):
    def operate(self, left, right) -> bool:
        return left < right


class ContainsOperator(ComparisonOperator, alias="contains"):
    def operate(self, left, right) -> bool:
        return right in left


class Comparator(Condition, alias="compare"):
    def __init__(
        self,
        left: StaticValueOrValueProvider,
        right: StaticValueOrValueProvider,
        operator: ComparisonOperator,
    ):
        self.left = ValueProvider.guarantee_value_provider(left)
        self.right = ValueProvider.guarantee_value_provider(right)
        self.operator = operator

    def evaluate(self, context: ProviderContext) -> bool:
        left = self.left.single_value(context)
        right = self.right.single_value(context)
        return self.operator.operate(left, right)

    @classmethod
    def from_file_data(cls, **arguments) -> "Condition":
        left = arguments["left"]
        right = arguments["right"]
        operator = COMPARISON_OPERATOR_SUBCLASS_REGISTRY.get(arguments["operator"])
        return cls(left=left, right=right, operator=operator())
