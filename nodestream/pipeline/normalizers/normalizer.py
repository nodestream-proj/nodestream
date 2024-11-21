from abc import ABC, abstractmethod
from functools import cache
from typing import Any

from ...pluggable import Pluggable
from ...subclass_registry import MissingFromRegistryError, SubclassRegistry

NORMALIZER_REGISTRY = SubclassRegistry()


class InvalidFlagError(ValueError):
    """Raised when a normalization flag is not valid."""

    def __init__(self, flag_name, *args: object) -> None:
        super().__init__(
            f"Normalization flag with name '{flag_name}' is not valid.`", *args
        )


@NORMALIZER_REGISTRY.connect_baseclass
class Normalizer(Pluggable, ABC):
    """A `Normalizer` is responsible for turning objects into a consistent form.

    When data is extracted from pipeline records from a value provider, the `Normalizer`
    is responsible for "cleaning up" the raw data such that is consistent. Often this comes
    in with regard to strings.
    """

    entrypoint_name = "normalizers"
    rejects_none_values = True

    @classmethod
    def setup(cls):
        pass

    @classmethod
    def normalize_by_args(cls, value: Any, normalizer_args) -> Any:
        if normalizer_args:
            for flag_name, enabled in normalizer_args.items():
                if enabled:
                    value = cls.by_flag_name(flag_name).normalize(value)

        return value

    @classmethod
    def argument_flag(cls):
        return f"do_{NORMALIZER_REGISTRY.name_for(cls)}"

    @classmethod
    def from_alias(cls, alias: str) -> "Normalizer":
        return NORMALIZER_REGISTRY.get(alias)()

    @classmethod
    @cache
    def by_flag_name(cls, flag_name: str) -> "Normalizer":
        try:
            if not flag_name.startswith("do_"):
                raise InvalidFlagError(flag_name)
            return cls.from_alias(flag_name[3:])
        except MissingFromRegistryError:
            raise InvalidFlagError(flag_name) from None

    def normalize(self, value: Any) -> Any:
        if value is None and self.rejects_none_values:
            return None
        return self.normalize_value(value)

    @abstractmethod
    def normalize_value(self, value: Any) -> Any:
        raise NotImplementedError
