from typing import Type

import pytest
from yaml import SafeLoader, load

from nodestream.pipeline.argument_resolvers import ArgumentResolver


@pytest.fixture
def yaml_loader_with_argument_resolver():
    def _yaml_loader_with_argument_resolver(resolver: Type[ArgumentResolver]):
        class ArgumentResolverLoader(SafeLoader):
            pass

        resolver.install_yaml_tag(ArgumentResolverLoader)
        return ArgumentResolverLoader

    return _yaml_loader_with_argument_resolver


@pytest.fixture
def load_file_with_resolver(yaml_loader_with_argument_resolver):
    def _load_file_with_resolver(file_path: str, resolver: Type[ArgumentResolver]):
        with open(file_path, "r") as f:
            return load(f, Loader=yaml_loader_with_argument_resolver(resolver))

    return _load_file_with_resolver
