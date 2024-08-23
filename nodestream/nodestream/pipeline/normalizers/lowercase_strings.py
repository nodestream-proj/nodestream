from typing import Any

from .normalizer import Normalizer


class LowercaseStrings(Normalizer, alias="lowercase_strings"):
    def normalize_value(self, value: Any) -> Any:
        return value.lower() if isinstance(value, str) else value
