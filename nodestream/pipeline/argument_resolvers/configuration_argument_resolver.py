from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional

from ..scope_config import ScopeConfig
from .argument_resolver import ArgumentResolver

config: ContextVar[Optional[ScopeConfig]] = ContextVar("context")


def get_config() -> Optional[ScopeConfig]:
    try:
        return config.get()
    except LookupError:
        return None


@contextmanager
def set_config(new_config: Optional[ScopeConfig]):
    token = config.set(new_config)
    try:
        yield
    finally:
        config.reset(token)


class ConfigurationArgumentResolver(ArgumentResolver, alias="config"):
    """An `ConfigurationArgumentResolver` is an `ArgumentResolver` that can resolves configuration values."""

    @staticmethod
    def resolve_argument(variable_name):
        if config := get_config():
            return config.get_config_value(variable_name)
        else:
            return None
