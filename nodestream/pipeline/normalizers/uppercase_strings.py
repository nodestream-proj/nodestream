from typing import Any

from .normalizer import Normalizer


class UppercaseStrings(Normalizer, alias="uppercase_strings"):
    def normalize_value(self, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value
