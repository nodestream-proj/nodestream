from hamcrest import assert_that, equal_to

from nodestream.pipeline import ScopeConfig
from nodestream.pipeline.argument_resolvers import (
    ConfigurationArgumentResolver,
    set_config,
)


def test_resolves_config_value():
    with set_config(ScopeConfig({"test": 1})):
        assert_that(ConfigurationArgumentResolver.resolve_argument("test"), equal_to(1))


def test_resolves_config_value_with_null_default():
    with set_config(ScopeConfig({"foo": 1})):
        assert_that(
            ConfigurationArgumentResolver.resolve_argument("test"),
            equal_to(None),
        )


def test_resolves_to_null_when_no_config():
    assert_that(
        ConfigurationArgumentResolver.resolve_argument("test"),
        equal_to(None),
    )
