from typing import Any

from .pipeline import Transformer


class Interpreter(Transformer):
    async def transform_record(self, record: Any):
        return await super().transform_record(record)
