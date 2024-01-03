from typing import Any

from ...pipeline.value_providers import ProviderContext, ValueProvider
from .transformer import Transformer


class ValueProjection(Transformer):
    def __init__(self, projection: ValueProvider) -> None:
        self.projection = projection

    async def transform_record(self, record: Any):
        context = ProviderContext.fresh(record)
        for result in self.projection.many_values(context):
            yield result
