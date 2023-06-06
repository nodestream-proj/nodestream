from typing import Any

from ..model import InterpreterContext
from ..pipeline import Transformer
from ..value_providers import ValueProvider


class ValueProjection(Transformer):
    def __init__(self, projection: ValueProvider) -> None:
        self.projection = projection

    async def transform_record(self, record: Any):
        context = InterpreterContext.fresh(record)
        for result in self.projection.many_values(context):
            yield result
