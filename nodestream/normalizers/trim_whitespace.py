from typing import Any

from .normalizer import Normalizer


class TrimWhitespace(Normalizer, name="trim_whitespace"):
    def normalize_value(self, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value
