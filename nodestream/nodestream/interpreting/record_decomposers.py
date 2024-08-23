from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Iterable, Optional

from ..pipeline.value_providers import ProviderContext, ValueProvider


class RecordDecomposer(ABC):
    """A RecordDecomposer is responsible for looking at a record to decomposing it to sub-records to look at"""

    @abstractmethod
    def decompose_record(self, context: ProviderContext) -> Iterable[ProviderContext]:
        raise NotImplementedError

    @classmethod
    def from_iteration_arguments(cls, iteration_arguments: Optional[ValueProvider]):
        if iteration_arguments:
            return ValueProviderDecomposer(iteration_arguments)

        return WholeRecordDecomposer()


class WholeRecordDecomposer(RecordDecomposer):
    """Simply returns the original `ProviderContext`.

    `decompose_record` will take the supplied `ProviderContext` and return it as the only
    decomposed record in the set.
    """

    def decompose_record(self, context: ProviderContext) -> Iterable[ProviderContext]:
        yield context


class ValueProviderDecomposer(RecordDecomposer):
    """Iterates on the values provided from a value provider from the root context.

    `decompose_record` will take the supplied `ProviderContext` and deep copy it for each
    provided value from the value provider when called on the supplied context.
    """

    def __init__(self, value_provider: ValueProvider) -> None:
        self.value_provider = value_provider

    def decompose_record(self, context: ProviderContext) -> Iterable[ProviderContext]:
        for sub_document in self.value_provider.many_values(context):
            sub_context = deepcopy(context)
            sub_context.document = sub_document
            yield sub_context
