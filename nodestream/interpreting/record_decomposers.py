from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Iterable, Optional

from ..model import InterpreterContext
from ..value_providers import ValueProvider


class RecordDecomposer(ABC):
    """A RecordDecomposer is responsible for looking at a record to decomposing it to sub-records to look at"""

    @abstractmethod
    def decompose_record(
        self, context: InterpreterContext
    ) -> Iterable[InterpreterContext]:
        raise NotImplementedError

    @classmethod
    def from_iteration_arguments(cls, iteration_arguments: Optional[ValueProvider]):
        if iteration_arguments:
            return ValueProviderDecomposer(iteration_arguments)

        return WholeRecordDecomposer()


class WholeRecordDecomposer(RecordDecomposer):
    """Simply returns the original `InterpreterContext`.

    `decompose_record` will take the supplied `InterpreterContext` and return it as the only
    decomposed record in the set.
    """

    def decompose_record(
        self, context: InterpreterContext
    ) -> Iterable[InterpreterContext]:
        yield context


class ValueProviderDecomposer(RecordDecomposer):
    """Iterates on the values provided from a value provider from the root context.

    `decompose_record` will take the supplied `InterpreterContext` and deep copy it for each
    provided value from the value provider when called on the supplied context.
    """

    def __init__(self, value_provider: ValueProvider) -> None:
        self.value_provider = value_provider

    def decompose_record(
        self, context: InterpreterContext
    ) -> Iterable[InterpreterContext]:
        for sub_document in self.value_provider.many_values(context):
            sub_context = deepcopy(context)
            sub_context.document = sub_document
            yield sub_context
