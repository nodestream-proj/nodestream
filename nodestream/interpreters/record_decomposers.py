from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Iterable, Optional

from ..model import IngestContext
from ..value_providers import ValueProvider


class RecordDecomposer(ABC):
    """ A RecordDecomposer is responsible for looking at a record to decomposing it to subrecords to look at"""
    
    @abstractmethod
    def decompose_record(self, context: IngestContext) -> Iterable[IngestContext]:
        ...

    @classmethod
    def from_iteration_arguments(cls, iteration_arguments: Optional[ValueProvider]):
        if iteration_arguments:
            return ValueProviderDecomposer(iteration_arguments)
        
        return WholeRecordDecomposer()


class WholeRecordDecomposer(RecordDecomposer):
    """ Simply returns the original `IngestContext`.

    `decompose_record` will take the supplied `IngestContext` and return it as the only
    decoposed record in the set.
    """
    def decompose_record(self, context: IngestContext) -> Iterable[IngestContext]:
        yield context


class ValueProviderDecomposer(RecordDecomposer):
    """ Iterates on the values provided from a value provider from the root context.

    `decompose_record` will take the supplied `IngestContext` and deep copy it for each 
    provided value from the value provider when called on the suppled context.
    """
    def __init__(self, value_provider: ValueProvider) -> None:
        self.value_provider = value_provider

    def decompose_record(self, context: IngestContext) -> Iterable[IngestContext]:
        for sub_document in self.value_provider.provide_many_values_from_context(
            context
        ):
            sub_context = deepcopy(context)
            sub_context.document = sub_document
            yield sub_context
