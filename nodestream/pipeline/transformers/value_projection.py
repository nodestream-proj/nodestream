from typing import Any, Optional

from ...pipeline.value_providers import ProviderContext, ValueProvider
from .transformer import Transformer


class ValueProjection(Transformer):
    def __init__(self, projection: ValueProvider) -> None:
        self.projection = projection

    async def transform_record(self, record: Any):
        context = ProviderContext.fresh(record)
        for result in self.projection.many_values(context):
            yield result

"""
Value projection with context:
"context": "stuff"
"iterate_on" [
    {"value": "1"}
    {"value": "2"}
    {"value": "3"}
]
->
{"context": "stuff", "value": "1"}
{"context": "stuff", "value": "2"}
{"context": "stuff", "value": "3"}

Additional Values define what context we would like to keep specifically.
If additional Values is not provided, we will assume that everything will be kept. 
"""

class ValueProjectionWithContext(Transformer):
    def __init__(
        self, projection: ValueProvider, additional_values: Optional[dict[str, ValueProvider]]
    ) -> None:
        self.projection = projection
        self.additional_values = additional_values

    def get_context(self, context):
        return {
            key: value.single_value(context)
            for key, value in self.additional_values.items()
        }

    async def transform_record(self, record: Any):
        context = ProviderContext.fresh(record)
        additional_values = self.get_context(context)
        for result in self.projection.many_values(context):
            result = dict(**additional_values, **result) 
            print(result)
            yield result