from typing import Any, Iterable
from nodestream.model import IngestContext
from nodestream.value_providers import ValueProvider


class StubbedValueProvider(ValueProvider):
    def __init__(self, values) -> None:
        self.values = values

    def provide_many_values_from_context(self, context: IngestContext) -> Iterable[Any]:
        return self.values
    
    def provide_single_value_from_context(self, context: IngestContext) -> Any:
        return self.values[0]
