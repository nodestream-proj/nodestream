from hamcrest import assert_that, equal_to

from nodestream.file_io import LazyLoadedArgument
from nodestream.pipeline.scope_config import ScopeConfig


def test_from_file_data_string_input():
    result = ScopeConfig.from_file_data({"TestKey": "TestValue"})
    assert_that(result.config, equal_to({"TestKey": "TestValue"}))


def test_target_resolves_lazy_tags(mocker):
    scope = ScopeConfig.from_file_data(
        {"user": LazyLoadedArgument("env", "USERNAME_ENV")}
    )
    mocker.patch("os.environ", {"USERNAME_ENV": "bob"})
    assert_that(scope.get_config_value("user"), equal_to("bob"))
