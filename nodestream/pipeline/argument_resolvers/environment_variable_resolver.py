import os

from .argument_resolver import ArgumentResolver


class EnvironmentResolver(ArgumentResolver, alias="env"):
    """An `EnvironmentResolver` is an `ArgumentResolver` that can resolve an environment variable into its value."""

    @staticmethod
    def resolve_argument(variable_name):
        return os.environ.get(variable_name)
