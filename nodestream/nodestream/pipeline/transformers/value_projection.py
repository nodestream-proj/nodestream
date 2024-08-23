from typing import Any, Dict

from ...pipeline.value_providers import ProviderContext, ValueProvider
from .transformer import Transformer


class ValueProjection(Transformer):
    def __init__(
        self,
        projection: ValueProvider,
        additional_values: Dict[str, ValueProvider] = {},
    ) -> None:
        self.projection = projection
        self.additional_values = additional_values

    def fetch_additional_values(self, context):
        return {
            key: provider.single_value(context)
            for key, provider in self.additional_values.items()
        }

    async def transform_record(self, record: Any):
        context = ProviderContext.fresh(record)
        for result in self.projection.many_values(context):
            yield dict(**self.fetch_additional_values(context), **result)
