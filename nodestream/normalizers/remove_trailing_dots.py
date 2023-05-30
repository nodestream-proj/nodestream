from typing import Any

from .normalizer import Normalizer


class RemoveTrailingDots(Normalizer, alias="remove_trailing_dots"):
    def normalize_value(self, value: Any) -> Any:
        return value.rstrip(".") if isinstance(value, str) else value
